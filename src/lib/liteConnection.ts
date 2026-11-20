/**
 * The connection to an openccu-lite system through occulite-client: the requests go to lite-rpc with the
 * token, the events of all its interfaces come from the stream the client opens, and `callbackEvents` turns them into the method
 * calls a callback server would make (`event`, `system.multicall`, `newDevices`, `deleteDevices`, ...) with
 * the same argument lists - so the adapter's handlers feed from the stream unchanged.
 *
 * occulite-client is an ES module; it is loaded here by a dynamic import, so the CCU path never loads it.
 */

import type { OccuLite, Status } from 'occulite-client';
import type { callbackEvents, CallbackServer } from 'occulite-client/callbacks';

/** A handler of the callback emitter */
type RpcCallback = Parameters<CallbackServer['on']>[1];
import type { nodeTransport, pair, probe } from 'occulite-client/node';
import { rpcCallbackError } from './lite';

/** The parts of occulite-client the lite mode uses, loaded once */
export interface LiteModules {
    OccuLite: typeof OccuLite;
    callbackEvents: typeof callbackEvents;
    nodeTransport: typeof nodeTransport;
    pair: typeof pair;
    probe: typeof probe;
}

/** Loads occulite-client, for the tests and the pairing a replaceable step */
export type LiteLoader = () => Promise<LiteModules>;

let loaded: Promise<LiteModules> | undefined;

/**
 * occulite-client, loaded once (an ES module, so by a dynamic import)
 */
export function loadOcculite(): Promise<LiteModules> {
    loaded ??= (async () => {
        const [client, callbacks, node] = await Promise.all([
            import('occulite-client'),
            import('occulite-client/callbacks'),
            import('occulite-client/node'),
        ]);
        return {
            OccuLite: client.OccuLite,
            callbackEvents: callbacks.callbackEvents,
            nodeTransport: node.nodeTransport,
            pair: node.pair,
            probe: node.probe,
        };
    })();
    return loaded;
}

/** The slice of a homematic-xmlrpc/binrpc client the adapter uses, served by lite-rpc */
export interface RpcClientLike {
    methodCall(method: string, params: unknown[], callback: (err: unknown, res: unknown) => void): void;
    close(): void;
}

export interface LiteConnectionOptions {
    /** the system, scheme and authority */
    url: string;
    /** the paired token */
    token: string;
    /** the certificate pinned at pairing (SHA-256), empty when the system is reached over plain HTTP */
    fingerprint?: string;
    /** the first argument of every callback method (the interface id the adapter expects), the same for every interface */
    initId: string;
    log: Pick<Console, 'debug' | 'info' | 'warn' | 'error'>;
    /** the client's state: connecting | ready | degraded | offline | closed */
    onStatus: (state: Status) => void;
    /** after a gap or a restart of the system the values were read again */
    onResync?: () => void;
    /** per request, ms */
    timeout?: number;
    loader?: LiteLoader;
}

/** Marks the interface in the interface id the callback emitter hands over, taken off again before the adapter sees it */
const IFACE_MARK = '\u0000';

/**
 * One instance's connection: every interface of the system, one stream, the callbacks of the adapter fed from it.
 *
 * The objects of all interfaces live side by side under the instance (`hm-rpc.0.<address>`), so the connection
 * keeps which interface each device belongs to: a request is sent to the interface of the address it names, and a
 * device whose address another interface already uses (none of the CCU's interfaces share an address format, so
 * this is not expected) is left out with a warning instead of mixing two devices in one object.
 */
export class LiteConnection {
    /** the client as the adapter's `rpcClient` */
    readonly client: RpcClientLike = {
        methodCall: (method, params, callback) => this.methodCall(method, params, callback),
        close: () => this.close(),
    };
    state: Status = 'idle';
    private box: OccuLite | undefined;
    private server: CallbackServer | undefined;
    private closed = false;
    /** device address → its interface, the first one that reported it */
    private readonly owners = new Map<string, string>();
    /** `<iface>.<device address>` of the devices left out because another interface has the address */
    private readonly clashes = new Set<string>();

    constructor(private readonly opts: LiteConnectionOptions) {}

    /**
     * Opens the connection. `register` gets the callback emitter before the stream starts, because the device
     * snapshot arrives as `newDevices` calls while the client connects. Resolves once the first picture is
     * loaded, rejects with the client's reason (unauthenticated, forbidden, unsupported, certificate, timeout).
     *
     * @param register registers the adapter's handlers on the callback emitter
     */
    async start(register: (server: CallbackServer) => void): Promise<void> {
        const modules = await (this.opts.loader ?? loadOcculite)();
        const trust = this.opts.fingerprint ? { fingerprint256: this.opts.fingerprint } : {};
        const box = new modules.OccuLite({
            url: this.opts.url,
            auth: { token: this.opts.token },
            transport: modules.nodeTransport(trust),
            timeout: this.opts.timeout,
            log: {
                debug: (m: string) => this.opts.log.debug(m),
                info: (m: string) => this.opts.log.debug(m),
                warn: (m: string) => this.opts.log.warn(m),
                error: (m: string) => this.opts.log.error(m),
            },
        });
        this.box = box;
        box.on('status', (s: { state: Status }) => {
            this.state = s.state;
            if (!this.closed) {
                this.opts.onStatus(s.state);
            }
        });
        box.on('interface', (ev: { interface: string; state: string }) =>
            this.opts.log.info(`lite-rpc <- interface ${ev.interface} ${ev.state}`),
        );
        box.on('error', (e: Error) => this.opts.log.warn(`lite-rpc: ${e.message}`));
        box.on('resync', (ev: { reason: string }) => {
            this.opts.log.info(`lite-rpc <- resync (${ev.reason}): the values were read again`);
            this.opts.onResync?.();
        });
        // before the callback emitter's own listener, so a device is known before its newDevices reaches the adapter
        box.on('devices', (ev: { kind: string; interface: string; refs: string[]; old?: string; new?: string }) =>
            this.trackDevices(ev),
        );
        this.server = modules.callbackEvents(box, { initId: (iface: string) => `${IFACE_MARK}${iface}` });
        register(this.routed(this.server));
        await box.ready();
    }

    /**
     * The interface a device or channel belongs to, undefined when no interface reported it
     *
     * @param address the address, e.g. `000A1B2C3D4E5F:1`
     */
    interfaceOf(address: string): string | undefined {
        return this.owners.get(deviceOf(address));
    }

    /**
     * Whether a device of an interface was left out because another interface has its address
     *
     * @param iface the interface
     * @param address the device's or a channel's address
     */
    private clashing(iface: string, address: string): boolean {
        return this.clashes.has(`${iface}.${deviceOf(address)}`);
    }

    /** Keeps the owners of the device addresses in step with the device events of the stream */
    private trackDevices(ev: { kind: string; interface: string; refs: string[]; old?: string; new?: string }): void {
        const addresses = ev.refs.map(ref => ref.slice(ref.indexOf('.') + 1));
        const claim = (address: string): void => {
            const device = deviceOf(address);
            const owner = this.owners.get(device);
            if (owner === undefined) {
                this.owners.set(device, ev.interface);
            } else if (owner !== ev.interface && !this.clashes.has(`${ev.interface}.${device}`)) {
                this.clashes.add(`${ev.interface}.${device}`);
                this.opts.log.warn(
                    `The device ${device} of ${ev.interface} is left out: ${owner} has a device with the same address`,
                );
            }
        };
        const release = (address: string): void => {
            if (!address.includes(':') && this.owners.get(address) === ev.interface) {
                this.owners.delete(address);
            }
            this.clashes.delete(`${ev.interface}.${address}`);
        };
        switch (ev.kind) {
            case 'new':
            case 'readded':
                addresses.forEach(claim);
                break;
            case 'deleted':
                addresses.forEach(release);
                break;
            case 'replaced':
                // the client names both, from the stream or from the addresses' order
                release(ev.old!);
                claim(ev.new!);
                break;
        }
    }

    /**
     * The callback emitter as the adapter sees it: the interface id is the adapter's again, the descriptions of a
     * newDevices carry their interface (`liteInterface`), and what belongs to a left-out device is dropped
     *
     * @param server occulite-client's callback emitter, its interface ids marked
     */
    private routed(server: CallbackServer): CallbackServer {
        const initId = this.opts.initId;
        const ifaceOf = (id: unknown): string => (typeof id === 'string' ? id.slice(IFACE_MARK.length) : '');
        return {
            on: (method: string, fn: RpcCallback) =>
                server.on(method, (err, params, callback) => {
                    if (method === 'system.multicall') {
                        const calls = (params[0] as { methodName: string; params: unknown[] }[]).filter(
                            c => !this.clashing(ifaceOf(c.params[0]), String(c.params[1])),
                        );
                        if (calls.length) {
                            fn(err, [calls.map(c => ({ ...c, params: [initId, ...c.params.slice(1)] }))], callback);
                        }
                        return;
                    }
                    const iface = ifaceOf(params[0]);
                    const rest = params.slice(1);
                    switch (method) {
                        case 'newDevices':
                            rest[0] = (rest[0] as { ADDRESS: string }[])
                                .filter(d => !this.clashing(iface, d.ADDRESS))
                                .map(d => ({ ...d, liteInterface: iface }));
                            break;
                        case 'deleteDevices':
                        case 'readdedDevice':
                            rest[0] = (rest[0] as string[]).filter(a => !this.clashing(iface, a));
                            break;
                        default:
                            // event, updateDevice, replaceDevice: one address
                            if (this.clashing(iface, String(rest[0]))) {
                                return;
                            }
                    }
                    fn(err, [initId, ...rest], callback);
                }),
            close: () => server.close(),
        };
    }

    /** The live copy of the system's names and taxonomy */
    get meta(): OccuLite['meta'] | undefined {
        return this.box?.meta;
    }

    /** The last value of every datapoint the system's state store keeps, keyed `<iface>.<address>.<KEY>` */
    get values(): OccuLite['values'] | undefined {
        return this.box?.values;
    }

    /** The device and channel descriptions the system holds now, keyed `<iface>.<address>`: the live picture */
    get devices(): OccuLite['devices'] | undefined {
        return this.box?.devices;
    }

    /** The interfaces followed, with their state */
    get interfaces(): { name: string; running: boolean }[] {
        return this.box?.interfaces ?? [];
    }

    /** Subscribes to the client's events (`meta`, `value`, ...); the answer unsubscribes */
    on(event: string, fn: (arg: any) => void): () => void {
        return this.box ? this.box.on(event, fn) : () => undefined;
    }

    /**
     * A method of the interface process through lite-rpc, in the callback shape of the RPC clients. A fault
     * of the process reaches the callback as its fault string, so the adapter's retry on `Failure` works.
     *
     * @param method the method name
     * @param params its parameters
     * @param callback (err, result)
     */
    methodCall(method: string, params: unknown[], callback: (err: unknown, res: unknown) => void): void {
        if (!this.box || this.closed) {
            callback('not connected', undefined);
            return;
        }
        const iface = typeof params[0] === 'string' ? this.interfaceOf(params[0]) : undefined;
        if (!iface) {
            callback(
                typeof params[0] === 'string'
                    ? `${method}: no interface of the system knows the address ${params[0]}`
                    : `${method}: names no address, the interface must be given`,
                undefined,
            );
            return;
        }
        this.callOn(iface, method, params, callback);
    }

    /**
     * A method of one interface process, for the requests that name no address (`listDevices`, `getInstallMode`,
     * ...)
     *
     * @param iface the interface, e.g. `BidCos-RF`
     * @param method the method name
     * @param params its parameters
     * @param callback (err, result)
     */
    callOn(iface: string, method: string, params: unknown[], callback: (err: unknown, res: unknown) => void): void {
        if (!this.box || this.closed) {
            callback('not connected', undefined);
            return;
        }
        this.box.call(iface, method, params).then(
            result => callback(null, result ?? ''),
            (e: unknown) => callback(rpcCallbackError(e), undefined),
        );
    }

    /** Closes the stream; nothing is de-registered, because nothing was registered */
    close(): void {
        this.closed = true;
        this.server?.close();
        this.server = undefined;
        this.box?.close();
        this.box = undefined;
    }
}

/**
 * The device a channel belongs to (the address itself for a device)
 *
 * @param address the address
 */
function deviceOf(address: string): string {
    const colon = address.indexOf(':');
    return colon < 0 ? address : address.slice(0, colon);
}
