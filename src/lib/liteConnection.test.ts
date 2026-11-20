import { expect } from 'chai';
import { afterEach, describe, it } from 'mocha';
import type { FakeBox } from 'occulite-client/testing';
import { LiteConnection, loadOcculite, type LiteModules } from './liteConnection';
import { DEVICE, DEVICES, IFACE, startFakeBox, TOKEN, waitFor } from './liteTestUtil';

type Call = { method: string; params: unknown[] };

function log(lines: string[] = []): Pick<Console, 'debug' | 'info' | 'warn' | 'error'> {
    const push = (level: string) => (m: string) => void lines.push(`${level} ${m}`);
    return { debug: push('debug'), info: push('info'), warn: push('warn'), error: push('error') };
}

describe('LiteConnection', function () {
    this.timeout(15_000);
    let box: FakeBox | undefined;
    let conn: LiteConnection | undefined;

    afterEach(async () => {
        conn?.close();
        conn = undefined;
        await box?.stop();
        box = undefined;
    });

    /** a connection to the fake box, its callbacks recorded */
    async function connect(
        opts: {
            token?: string;
            fingerprint?: string;
            loader?: () => Promise<LiteModules>;
            onResync?: () => void;
            lines?: string[];
        } = {},
    ): Promise<{ calls: Call[]; statuses: string[] }> {
        const calls: Call[] = [];
        const statuses: string[] = [];
        conn = new LiteConnection({
            url: box!.url,
            token: opts.token ?? TOKEN,
            fingerprint: opts.fingerprint,
            initId: 'hm-rpc.0',
            log: log(opts.lines),
            onStatus: s => statuses.push(s),
            onResync: opts.onResync,
            loader: opts.loader,
        });
        await conn.start(server => {
            for (const method of [
                'event',
                'system.multicall',
                'newDevices',
                'deleteDevices',
                'updateDevice',
                'replaceDevice',
                'readdedDevice',
            ]) {
                server.on(method, (err, params, cb) => {
                    calls.push({ method, params });
                    cb(null, '');
                });
            }
        });
        return { calls, statuses };
    }

    it('follows the devices with a token that holds a higher rpc tier alone', async () => {
        box = await startFakeBox({ scopes: ['meta:read', 'rpc:configure'] });
        const { calls, statuses } = await connect();
        expect(statuses).to.deep.equal(['connecting', 'ready']);
        expect(calls.some(c => c.method === 'newDevices')).to.equal(true);
        expect(conn!.values!.get(`${IFACE}.${DEVICE}:1.STATE`)!.value).to.equal(true);
    });

    it('connects, delivers the device snapshot as newDevices and follows the status', async () => {
        box = await startFakeBox();
        const { calls, statuses } = await connect();
        expect(statuses).to.deep.equal(['connecting', 'ready']);
        expect(conn!.state).to.equal('ready');
        const snapshot = calls.find(c => c.method === 'newDevices')!;
        expect(snapshot.params[0]).to.equal('hm-rpc.0');
        expect((snapshot.params[1] as { ADDRESS: string }[]).map(d => d.ADDRESS)).to.deep.equal([
            DEVICE,
            `${DEVICE}:0`,
            `${DEVICE}:1`,
        ]);
        expect(conn!.meta!.name(`${IFACE}.${DEVICE}`)).to.equal('Stehlampe');
        expect(conn!.values!.get(`${IFACE}.${DEVICE}:1.STATE`)!.value).to.equal(true);
    });

    it('turns the events of the stream into the callback methods with the interface id of the adapter', async () => {
        box = await startFakeBox();
        const { calls } = await connect();
        box.event(IFACE, `${DEVICE}:1`, 'STATE', false);
        await waitFor(() => calls.some(c => c.method === 'event'));
        expect(calls.find(c => c.method === 'event')!.params).to.deep.equal([
            'hm-rpc.0',
            `${DEVICE}:1`,
            'STATE',
            false,
        ]);

        // a multicall: the events of one batch together
        const batch = box.nextSeq();
        box.event(IFACE, `${DEVICE}:0`, 'UNREACH', true, batch);
        box.event(IFACE, `${DEVICE}:0`, 'LOW_BAT', true, batch);
        await waitFor(() => calls.some(c => c.method === 'system.multicall'));
        const multicall = calls.find(c => c.method === 'system.multicall')!;
        expect(multicall.params[0]).to.deep.equal([
            { methodName: 'event', params: ['hm-rpc.0', `${DEVICE}:0`, 'UNREACH', true] },
            { methodName: 'event', params: ['hm-rpc.0', `${DEVICE}:0`, 'LOW_BAT', true] },
        ]);

        box.push('deleteDevices', { interface: IFACE, addresses: [`${DEVICE}:1`] });
        box.updateDevice(IFACE, DEVICE, 2);
        box.replaceDevice(IFACE, DEVICE, 'NEW000000001');
        box.push('readdedDevice', { interface: IFACE, addresses: [DEVICE] });
        await waitFor(() => calls.some(c => c.method === 'readdedDevice'));
        expect(calls.find(c => c.method === 'deleteDevices')!.params).to.deep.equal(['hm-rpc.0', [`${DEVICE}:1`]]);
        expect(calls.find(c => c.method === 'updateDevice')!.params).to.deep.equal(['hm-rpc.0', DEVICE, 2]);
        expect(calls.find(c => c.method === 'replaceDevice')!.params).to.deep.equal([
            'hm-rpc.0',
            DEVICE,
            'NEW000000001',
        ]);
        expect(calls.find(c => c.method === 'readdedDevice')!.params).to.deep.equal(['hm-rpc.0', [DEVICE]]);
    });

    it('calls the interface process through lite-rpc in the callback shape of the RPC clients', async () => {
        box = await startFakeBox();
        await connect();
        const result = await new Promise((res, rej) =>
            conn!.client.methodCall('setValue', [`${DEVICE}:1`, 'STATE', true], (err, r) =>
                err ? rej(new Error(err as string)) : res(r),
            ),
        );
        expect(result).to.equal('');
        expect(box.calls.at(-1)).to.deep.equal({
            interface: IFACE,
            method: 'setValue',
            params: [`${DEVICE}:1`, 'STATE', true],
        });

        // a method without an address goes to the interface named
        const list = await new Promise((res, rej) =>
            conn!.callOn(IFACE, 'listDevices', [], (err, r) => (err ? rej(new Error(err as string)) : res(r))),
        );
        expect((list as unknown[]).length).to.equal(3);
        // a null result (getValue of an unknown datapoint) is '' as the RPC clients give it
        const nothing = await new Promise((res, rej) =>
            conn!.methodCall('getValue', [`${DEVICE}:1`, 'NOPE'], (err, r) =>
                err ? rej(new Error(err as string)) : res(r),
            ),
        );
        expect(nothing).to.equal('');
        // without an address, or with one no interface knows, there is nowhere to send it
        expect(await new Promise(res => conn!.methodCall('listDevices', [], err => res(err)))).to.equal(
            'listDevices: names no address, the interface must be given',
        );
        expect(await new Promise(res => conn!.methodCall('getValue', ['NOPE:1', 'STATE'], err => res(err)))).to.equal(
            'getValue: no interface of the system knows the address NOPE:1',
        );
        expect(box.calls.some(c => c.params[0] === 'NOPE:1')).to.equal(false);
    });

    it('hands a fault of the process over as its fault string, a client error with its code', async () => {
        box = await startFakeBox({ failing: [`${IFACE}.${DEVICE}:1`] });
        await connect();
        const fault = await new Promise(res =>
            conn!.methodCall('getParamset', [`${DEVICE}:1`, 'VALUES'], err => res(err)),
        );
        expect(fault).to.equal('Failure');
        // an unknown method is a fault of the process too (JSON-RPC -32601)
        const unknown = await new Promise(res => conn!.callOn(IFACE, 'noSuchMethod', [], err => res(err)));
        expect(unknown).to.equal('unknown method noSuchMethod');
        // a system that is gone: the client's code names the kind
        await box.stop();
        box = undefined;
        const gone = await new Promise(res => conn!.callOn(IFACE, 'ping', [], err => res(err)));
        expect(gone).to.match(/^unreachable: /);
    });

    it('answers "not connected" before start and after close', async () => {
        box = await startFakeBox();
        const early = new LiteConnection({
            url: box.url,
            token: TOKEN,
            initId: 'hm-rpc.0',
            log: log(),
            onStatus: () => undefined,
        });
        expect(early.meta).to.be.undefined;
        expect(early.values).to.be.undefined;
        expect(typeof early.on('meta', () => undefined)).to.equal('function');
        expect(await new Promise(res => early.methodCall('ping', [], err => res(err)))).to.equal('not connected');
        expect(await new Promise(res => early.callOn(IFACE, 'ping', [], err => res(err)))).to.equal('not connected');
        expect(early.devices).to.be.undefined;
        expect(early.interfaces).to.deep.equal([]);

        const { statuses } = await connect();
        conn!.close();
        expect(await new Promise(res => conn!.client.methodCall('ping', [], err => res(err)))).to.equal(
            'not connected',
        );
        // the closed status is not reported: the adapter is shutting down
        expect(statuses).to.deep.equal(['connecting', 'ready']);
        conn!.client.close(); // a second close is harmless
    });

    it('reports a lost stream as degraded and the reconnect as ready, a restart of the system as resync', async () => {
        box = await startFakeBox();
        let resyncs = 0;
        const lines: string[] = [];
        const { statuses } = await connect({ onResync: () => resyncs++, lines });
        box.dropStreams();
        await waitFor(() => statuses.includes('degraded'));
        await waitFor(() => statuses.at(-1) === 'ready', 5000, 'the reconnect');
        expect(box.streamsOpened).to.be.greaterThan(1);

        box.bootId = 'boot2';
        box.dropStreams();
        await waitFor(() => resyncs > 0, 5000, 'the resync');
        expect(lines.some(l => l.startsWith('info lite-rpc <- resync (boot)'))).to.equal(true);

        box.setInterface(IFACE, false);
        await waitFor(() => lines.some(l => l === `info lite-rpc <- interface ${IFACE} down`));
    });

    it('goes offline when the token is refused, with the error logged', async () => {
        box = await startFakeBox();
        const lines: string[] = [];
        const { statuses } = await connect({ lines });
        box.opts.token = 'olt_another';
        box.dropStreams();
        await waitFor(() => statuses.includes('offline'), 5000, 'offline');
        expect(lines.some(l => l.startsWith('warn lite-rpc: '))).to.equal(true);
    });

    it('pins the certificate of the pairing (no effect over plain HTTP)', async () => {
        box = await startFakeBox();
        const { statuses } = await connect({ fingerprint: 'AB:CD:EF' });
        expect(statuses).to.deep.equal(['connecting', 'ready']);
    });

    it('rejects start() with the reason when the credential is wrong', async () => {
        box = await startFakeBox();
        let code: string | undefined;
        try {
            await connect({ token: 'olt_wrong' });
        } catch (e) {
            code = (e as { code?: string }).code;
        }
        expect(code).to.equal('unauthenticated');
    });

    describe('several interfaces', () => {
        const BIDCOS = 'BidCos-RF';
        const BIDCOS_DEVICES = [
            {
                ADDRESS: 'MEQ0123456',
                TYPE: 'HM-LC-Sw1-Pl-2',
                VERSION: 1,
                CHILDREN: ['MEQ0123456:1'],
                PARAMSETS: ['MASTER'],
            },
            {
                ADDRESS: 'MEQ0123456:1',
                TYPE: 'SWITCH',
                PARENT: 'MEQ0123456',
                PARENT_TYPE: 'HM-LC-Sw1-Pl-2',
                VERSION: 1,
                PARAMSETS: ['MASTER', 'VALUES'],
            },
        ];

        it('follows every interface: the snapshots marked with their interface, each request to its interface', async () => {
            box = await startFakeBox({
                interfaces: [IFACE, BIDCOS],
                devices: { [IFACE]: structuredClone(DEVICES), [BIDCOS]: structuredClone(BIDCOS_DEVICES) },
            });
            const { calls } = await connect();
            const snapshots = calls.filter(c => c.method === 'newDevices');
            expect(snapshots.map(c => c.params[0])).to.deep.equal(['hm-rpc.0', 'hm-rpc.0']);
            const tagged = snapshots.flatMap(c => c.params[1] as { ADDRESS: string; liteInterface: string }[]);
            expect(tagged.find(d => d.ADDRESS === 'MEQ0123456:1')!.liteInterface).to.equal(BIDCOS);
            expect(tagged.find(d => d.ADDRESS === DEVICE)!.liteInterface).to.equal(IFACE);
            expect(conn!.interfaceOf('MEQ0123456:1')).to.equal(BIDCOS);
            expect(conn!.interfaceOf(`${DEVICE}:1`)).to.equal(IFACE);
            expect(conn!.interfaceOf('NOPE')).to.be.undefined;
            expect(conn!.interfaces.map(i => i.name).sort()).to.deep.equal([BIDCOS, IFACE]);

            await new Promise(res => conn!.methodCall('setValue', ['MEQ0123456:1', 'STATE', true], res));
            await new Promise(res => conn!.methodCall('setValue', [`${DEVICE}:1`, 'STATE', true], res));
            expect(box.calls.slice(-2).map(c => c.interface)).to.deep.equal([BIDCOS, IFACE]);

            box.event(BIDCOS, 'MEQ0123456:1', 'STATE', false);
            await waitFor(() => calls.some(c => c.method === 'event'));
            expect(calls.find(c => c.method === 'event')!.params).to.deep.equal([
                'hm-rpc.0',
                'MEQ0123456:1',
                'STATE',
                false,
            ]);
        });

        it('follows the devices of an interface as they come and go, and a replaced one', async () => {
            box = await startFakeBox({
                interfaces: [IFACE, BIDCOS],
                devices: { [IFACE]: structuredClone(DEVICES), [BIDCOS]: structuredClone(BIDCOS_DEVICES) },
            });
            const { calls } = await connect();
            box.push('deleteDevices', { interface: BIDCOS, addresses: ['MEQ0123456', 'MEQ0123456:1'] });
            await waitFor(() => calls.some(c => c.method === 'deleteDevices'));
            expect(conn!.interfaceOf('MEQ0123456')).to.be.undefined;
            box.push('readdedDevice', { interface: BIDCOS, addresses: ['MEQ0123456'] });
            await waitFor(() => calls.some(c => c.method === 'readdedDevice'));
            expect(conn!.interfaceOf('MEQ0123456:1')).to.equal(BIDCOS);
            box.replaceDevice(BIDCOS, 'MEQ0123456', 'MEQ0654321');
            await waitFor(() => calls.some(c => c.method === 'replaceDevice'));
            expect(conn!.interfaceOf('MEQ0123456')).to.be.undefined;
            expect(conn!.interfaceOf('MEQ0654321')).to.equal(BIDCOS);
        });

        it('leaves out a device whose address another interface uses, with a warning, and all that belongs to it', async () => {
            const twin = structuredClone(DEVICES).map(d => ({ ...d, TYPE: `X-${String(d.TYPE)}` }));
            box = await startFakeBox({
                interfaces: [IFACE, BIDCOS],
                devices: { [IFACE]: structuredClone(DEVICES), [BIDCOS]: twin },
            });
            const lines: string[] = [];
            const { calls } = await connect({ lines });
            const descriptions = calls
                .filter(c => c.method === 'newDevices')
                .flatMap(c => c.params[1] as { ADDRESS: string; liteInterface: string }[]);
            const owner = conn!.interfaceOf(DEVICE)!;
            const other = owner === IFACE ? BIDCOS : IFACE;
            expect(descriptions.filter(d => d.ADDRESS === DEVICE).map(d => d.liteInterface)).to.deep.equal([owner]);
            expect(lines.filter(l => l.includes('is left out'))).to.deep.equal([
                `warn The device ${DEVICE} of ${other} is left out: ${owner} has a device with the same address`,
            ]);

            // the events of the left-out one are dropped, single and in a multicall
            box.event(other, `${DEVICE}:1`, 'STATE', false);
            const batch = box.nextSeq();
            box.event(other, `${DEVICE}:0`, 'UNREACH', true, batch);
            box.event(owner, `${DEVICE}:0`, 'UNREACH', true, batch);
            await waitFor(() => calls.some(c => c.method === 'system.multicall'));
            await new Promise(res => setTimeout(res, 100));
            expect(calls.filter(c => c.method === 'event')).to.deep.equal([]);
            expect(calls.filter(c => c.method === 'system.multicall').map(c => c.params[0])).to.deep.equal([
                [{ methodName: 'event', params: ['hm-rpc.0', `${DEVICE}:0`, 'UNREACH', true] }],
            ]);

            box.updateDevice(other, DEVICE, 2);
            box.push('deleteDevices', { interface: other, addresses: [`${DEVICE}:1`] });
            box.updateDevice(owner, DEVICE, 2);
            await waitFor(() => calls.some(c => c.method === 'updateDevice'));
            await new Promise(res => setTimeout(res, 100));
            expect(calls.filter(c => c.method === 'updateDevice').map(c => c.params)).to.deep.equal([
                ['hm-rpc.0', DEVICE, 2],
            ]);
            expect(calls.filter(c => c.method === 'deleteDevices').map(c => c.params)).to.deep.equal([
                ['hm-rpc.0', []],
            ]);
            // the left-out one goes: its address is free for it when it comes again
            box.push('deleteDevices', { interface: other, addresses: [DEVICE] });
            await waitFor(() => calls.filter(c => c.method === 'deleteDevices').length === 2);
            expect(conn!.interfaceOf(DEVICE)).to.equal(owner);
        });
    });

    it('takes a loader of its own and logs through the adapter', async () => {
        box = await startFakeBox({ interfacesRoute: false });
        const lines: string[] = [];
        let loads = 0;
        const loader = async (): Promise<LiteModules> => {
            loads++;
            return loadOcculite();
        };
        await connect({ loader, lines });
        expect(loads).to.equal(1);
        // the client's own log lines go to the adapter's log
        expect(lines.some(l => l.startsWith('warn occulite-client: the interfaces list is not available'))).to.equal(
            true,
        );
    });
});
