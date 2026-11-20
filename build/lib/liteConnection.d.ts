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
import type { nodeTransport, pair, probe } from 'occulite-client/node';
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
/**
 * occulite-client, loaded once (an ES module, so by a dynamic import)
 */
export declare function loadOcculite(): Promise<LiteModules>;
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
/**
 * One instance's connection: every interface of the system, one stream, the callbacks of the adapter fed from it.
 *
 * The objects of all interfaces live side by side under the instance (`hm-rpc.0.<address>`), so the connection
 * keeps which interface each device belongs to: a request is sent to the interface of the address it names, and a
 * device whose address another interface already uses (none of the CCU's interfaces share an address format, so
 * this is not expected) is left out with a warning instead of mixing two devices in one object.
 */
export declare class LiteConnection {
    private readonly opts;
    /** the client as the adapter's `rpcClient` */
    readonly client: RpcClientLike;
    state: Status;
    private box;
    private server;
    private closed;
    /** device address → its interface, the first one that reported it */
    private readonly owners;
    /** `<iface>.<device address>` of the devices left out because another interface has the address */
    private readonly clashes;
    constructor(opts: LiteConnectionOptions);
    /**
     * Opens the connection. `register` gets the callback emitter before the stream starts, because the device
     * snapshot arrives as `newDevices` calls while the client connects. Resolves once the first picture is
     * loaded, rejects with the client's reason (unauthenticated, forbidden, unsupported, certificate, timeout).
     *
     * @param register registers the adapter's handlers on the callback emitter
     */
    start(register: (server: CallbackServer) => void): Promise<void>;
    /**
     * The interface a device or channel belongs to, undefined when no interface reported it
     *
     * @param address the address, e.g. `000A1B2C3D4E5F:1`
     */
    interfaceOf(address: string): string | undefined;
    /**
     * Whether a device of an interface was left out because another interface has its address
     *
     * @param iface the interface
     * @param address the device's or a channel's address
     */
    private clashing;
    /** Keeps the owners of the device addresses in step with the device events of the stream */
    private trackDevices;
    /**
     * The callback emitter as the adapter sees it: the interface id is the adapter's again, the descriptions of a
     * newDevices carry their interface (`liteInterface`), and what belongs to a left-out device is dropped
     *
     * @param server occulite-client's callback emitter, its interface ids marked
     */
    private routed;
    /** The live copy of the system's names and taxonomy */
    get meta(): OccuLite['meta'] | undefined;
    /** The last value of every datapoint the system's state store keeps, keyed `<iface>.<address>.<KEY>` */
    get values(): OccuLite['values'] | undefined;
    /** The device and channel descriptions the system holds now, keyed `<iface>.<address>`: the live picture */
    get devices(): OccuLite['devices'] | undefined;
    /** The interfaces followed, with their state */
    get interfaces(): {
        name: string;
        running: boolean;
    }[];
    /** Subscribes to the client's events (`meta`, `value`, ...); the answer unsubscribes */
    on(event: string, fn: (arg: any) => void): () => void;
    /**
     * A method of the interface process through lite-rpc, in the callback shape of the RPC clients. A fault
     * of the process reaches the callback as its fault string, so the adapter's retry on `Failure` works.
     *
     * @param method the method name
     * @param params its parameters
     * @param callback (err, result)
     */
    methodCall(method: string, params: unknown[], callback: (err: unknown, res: unknown) => void): void;
    /**
     * A method of one interface process, for the requests that name no address (`listDevices`, `getInstallMode`,
     * ...)
     *
     * @param iface the interface, e.g. `BidCos-RF`
     * @param method the method name
     * @param params its parameters
     * @param callback (err, result)
     */
    callOn(iface: string, method: string, params: unknown[], callback: (err: unknown, res: unknown) => void): void;
    /** Closes the stream; nothing is de-registered, because nothing was registered */
    close(): void;
}
