"use strict";
/**
 * The connection to an openccu-lite system through occulite-client: the requests go to lite-rpc with the
 * token, the events of all its interfaces come from the stream the client opens, and `callbackEvents` turns them into the method
 * calls a callback server would make (`event`, `system.multicall`, `newDevices`, `deleteDevices`, ...) with
 * the same argument lists - so the adapter's handlers feed from the stream unchanged.
 *
 * occulite-client is an ES module; it is loaded here by a dynamic import, so the CCU path never loads it.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.LiteConnection = void 0;
exports.loadOcculite = loadOcculite;
const lite_1 = require("./lite");
let loaded;
/**
 * occulite-client, loaded once (an ES module, so by a dynamic import)
 */
function loadOcculite() {
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
class LiteConnection {
    constructor(opts) {
        this.opts = opts;
        /** the client as the adapter's `rpcClient` */
        this.client = {
            methodCall: (method, params, callback) => this.methodCall(method, params, callback),
            close: () => this.close(),
        };
        this.state = 'idle';
        this.closed = false;
        /** device address → its interface, the first one that reported it */
        this.owners = new Map();
        /** `<iface>.<device address>` of the devices left out because another interface has the address */
        this.clashes = new Set();
    }
    /**
     * Opens the connection. `register` gets the callback emitter before the stream starts, because the device
     * snapshot arrives as `newDevices` calls while the client connects. Resolves once the first picture is
     * loaded, rejects with the client's reason (unauthenticated, forbidden, unsupported, certificate, timeout).
     *
     * @param register registers the adapter's handlers on the callback emitter
     */
    async start(register) {
        const modules = await (this.opts.loader ?? loadOcculite)();
        const trust = this.opts.fingerprint ? { fingerprint256: this.opts.fingerprint } : {};
        const box = new modules.OccuLite({
            url: this.opts.url,
            auth: { token: this.opts.token },
            transport: modules.nodeTransport(trust),
            timeout: this.opts.timeout,
            log: {
                debug: (m) => this.opts.log.debug(m),
                info: (m) => this.opts.log.debug(m),
                warn: (m) => this.opts.log.warn(m),
                error: (m) => this.opts.log.error(m),
            },
        });
        this.box = box;
        box.on('status', (s) => {
            this.state = s.state;
            if (!this.closed) {
                this.opts.onStatus(s.state);
            }
        });
        box.on('interface', (ev) => this.opts.log.info(`lite-rpc <- interface ${ev.interface} ${ev.state}`));
        box.on('error', (e) => this.opts.log.warn(`lite-rpc: ${e.message}`));
        box.on('resync', (ev) => {
            this.opts.log.info(`lite-rpc <- resync (${ev.reason}): the values were read again`);
            this.opts.onResync?.();
        });
        // before the callback emitter's own listener, so a device is known before its newDevices reaches the adapter
        box.on('devices', (ev) => this.trackDevices(ev));
        this.server = modules.callbackEvents(box, { initId: (iface) => `${IFACE_MARK}${iface}` });
        register(this.routed(this.server));
        await box.ready();
    }
    /**
     * The interface a device or channel belongs to, undefined when no interface reported it
     *
     * @param address the address, e.g. `000A1B2C3D4E5F:1`
     */
    interfaceOf(address) {
        return this.owners.get(deviceOf(address));
    }
    /**
     * Whether a device of an interface was left out because another interface has its address
     *
     * @param iface the interface
     * @param address the device's or a channel's address
     */
    clashing(iface, address) {
        return this.clashes.has(`${iface}.${deviceOf(address)}`);
    }
    /** Keeps the owners of the device addresses in step with the device events of the stream */
    trackDevices(ev) {
        const addresses = ev.refs.map(ref => ref.slice(ref.indexOf('.') + 1));
        const claim = (address) => {
            const device = deviceOf(address);
            const owner = this.owners.get(device);
            if (owner === undefined) {
                this.owners.set(device, ev.interface);
            }
            else if (owner !== ev.interface && !this.clashes.has(`${ev.interface}.${device}`)) {
                this.clashes.add(`${ev.interface}.${device}`);
                this.opts.log.warn(`The device ${device} of ${ev.interface} is left out: ${owner} has a device with the same address`);
            }
        };
        const release = (address) => {
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
                release(ev.old);
                claim(ev.new);
                break;
        }
    }
    /**
     * The callback emitter as the adapter sees it: the interface id is the adapter's again, the descriptions of a
     * newDevices carry their interface (`liteInterface`), and what belongs to a left-out device is dropped
     *
     * @param server occulite-client's callback emitter, its interface ids marked
     */
    routed(server) {
        const initId = this.opts.initId;
        const ifaceOf = (id) => (typeof id === 'string' ? id.slice(IFACE_MARK.length) : '');
        return {
            on: (method, fn) => server.on(method, (err, params, callback) => {
                if (method === 'system.multicall') {
                    const calls = params[0].filter(c => !this.clashing(ifaceOf(c.params[0]), String(c.params[1])));
                    if (calls.length) {
                        fn(err, [calls.map(c => ({ ...c, params: [initId, ...c.params.slice(1)] }))], callback);
                    }
                    return;
                }
                const iface = ifaceOf(params[0]);
                const rest = params.slice(1);
                switch (method) {
                    case 'newDevices':
                        rest[0] = rest[0]
                            .filter(d => !this.clashing(iface, d.ADDRESS))
                            .map(d => ({ ...d, liteInterface: iface }));
                        break;
                    case 'deleteDevices':
                    case 'readdedDevice':
                        rest[0] = rest[0].filter(a => !this.clashing(iface, a));
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
    get meta() {
        return this.box?.meta;
    }
    /** The last value of every datapoint the system's state store keeps, keyed `<iface>.<address>.<KEY>` */
    get values() {
        return this.box?.values;
    }
    /** The device and channel descriptions the system holds now, keyed `<iface>.<address>`: the live picture */
    get devices() {
        return this.box?.devices;
    }
    /** The interfaces followed, with their state */
    get interfaces() {
        return this.box?.interfaces ?? [];
    }
    /** Subscribes to the client's events (`meta`, `value`, ...); the answer unsubscribes */
    on(event, fn) {
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
    methodCall(method, params, callback) {
        if (!this.box || this.closed) {
            callback('not connected', undefined);
            return;
        }
        const iface = typeof params[0] === 'string' ? this.interfaceOf(params[0]) : undefined;
        if (!iface) {
            callback(typeof params[0] === 'string'
                ? `${method}: no interface of the system knows the address ${params[0]}`
                : `${method}: names no address, the interface must be given`, undefined);
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
    callOn(iface, method, params, callback) {
        if (!this.box || this.closed) {
            callback('not connected', undefined);
            return;
        }
        this.box.call(iface, method, params).then(result => callback(null, result ?? ''), (e) => callback((0, lite_1.rpcCallbackError)(e), undefined));
    }
    /** Closes the stream; nothing is de-registered, because nothing was registered */
    close() {
        this.closed = true;
        this.server?.close();
        this.server = undefined;
        this.box?.close();
        this.box = undefined;
    }
}
exports.LiteConnection = LiteConnection;
/**
 * The device a channel belongs to (the address itself for a device)
 *
 * @param address the address
 */
function deviceOf(address) {
    const colon = address.indexOf(':');
    return colon < 0 ? address : address.slice(0, colon);
}
//# sourceMappingURL=liteConnection.js.map