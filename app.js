const net = require('net');
const http = require('http');
const { Buffer } = require('buffer');
const { execSync } = require('child_process'); // Retain execSync for initial config setup

// Tianxie bianliang ceshi zhengchang hou, quanxuan daima dao https://obfuscator.io quanju hunxiao baocun bushu
const UUID = process.env.UUID || 'ea58ceb6-c772-4b72-8262-7911c54c0358'; // UUID
const SUB_PATH = process.env.SUB_PATH || 'sub';            // Jiedian dingyue luxiang
const XPATH = process.env.XPATH || UUID.slice(0, 8);       // Jiedian luxiang
const DOMAIN = process.env.DOMAIN || 'niangthree-fenchattp.hf.space';   // Bitianxiang, hug fenpei de yuming huozhe workers fandaidaihou de yuming, liru: xxx.abc-hf.space
const NAME = process.env.NAME || 'Hf';
const PORT = process.env.PORT || 7860;

// Yiji zhongyanghua de peizhi shezhi
const SETTINGS = {
    UUID: UUID,
    LOG_LEVEL: 'info', // Geng gao de rizhi jibie yi fangbian diaoshi
    BUFFER_SIZE: 1024,
    XPATH: `%2F${XPATH}`,
    MAX_BUFFERED_POSTS: 30,
    MAX_POST_SIZE: 1000000,
    SESSION_TIMEOUT: 30000,
    CHUNK_SIZE: 1024 * 1024,
    TCP_NODELAY: true,
    TCP_KEEPALIVE: true,
};

/**
 * Yanzheng liang ge UUID shifou xiangtong
 * @param {Uint8Array} left
 * @param {Uint8Array} right
 * @returns {boolean}
 */
function validate_uuid(left, right) {
    if (left.length !== 16 || right.length !== 16) return false;
    for (let i = 0; i < 16; i++) {
        if (left[i] !== right[i]) return false;
    }
    return true;
}

/**
 * Lianzhe duo ge TypedArray
 * @param {Uint8Array} first
 * @param {...Uint8Array} args
 * @returns {Uint8Array}
 */
function concat_typed_arrays(first, ...args) {
    if (!args || args.length < 1) return first;
    let len = first.length;
    for (const a of args) len += a.length;
    const r = new first.constructor(len);
    r.set(first, 0);
    let offset = first.length;
    for (const a of args) {
        r.set(a, offset);
        offset += a.length;
    }
    return r;
}

/**
 * Tongyi de rizhi shuchu han shu
 * @param {'debug'|'info'|'warn'|'error'} type
 * @param {...any} args
 */
function log(type, ...args) {
    if (SETTINGS.LOG_LEVEL === 'none') return;

    const levels = {
        'debug': 0,
        'info': 1,
        'warn': 2,
        'error': 3
    };

    const colors = {
        'debug': '\x1b[36m', // Qing se
        'info': '\x1b[32m',  // Lu se
        'warn': '\x1b[33m',  // Huang se
        'error': '\x1b[31m', // Hong se
        'reset': '\x1b[0m'   // Chong zhi yan se
    };

    const configLevel = levels[SETTINGS.LOG_LEVEL] || 1;
    const messageLevel = levels[type] || 0;

    if (messageLevel >= configLevel) {
        const time = new Date().toISOString();
        const color = colors[type] || colors.reset;
        console.log(`${color}[${time}] [${type.toUpperCase()}]`, ...args, colors.reset);
    }
}

/**
 * Jiexi UUID zi fu chuan wei Uint8Array
 * @param {string} uuidStr
 * @returns {Uint8Array}
 */
function parse_uuid(uuidStr) {
    const cleanedUuid = uuidStr.replaceAll('-', '');
    const result = new Uint8Array(16);
    for (let i = 0; i < 16; i++) {
        result[i] = parseInt(cleanedUuid.substring(i * 2, i * 2 + 2), 16);
    }
    return result;
}

/**
 * Cong du qu qi du qu zhi shao n ge zi jie
 * @param {ReadableStreamDefaultReader} reader
 * @param {number} n
 * @returns {Promise<{value: Uint8Array, done: boolean}>}
 */
async function read_atleast(reader, n) {
    const chunks = [];
    let bytesRead = 0;
    let done = false;

    while (bytesRead < n && !done) {
        const { value, done: readerDone } = await reader.read();
        if (value) {
            chunks.push(new Uint8Array(value));
            bytesRead += value.length;
        }
        done = readerDone;
    }

    if (bytesRead < n) {
        throw new Error(`not enough data to read, expected ${n} but got ${bytesRead}`);
    }

    return {
        value: concat_typed_arrays(...chunks),
        done: done
    };
}

/**
 * Jiexi VLESS xie yi tou bu
 * @param {ReadableStreamDefaultReader} reader
 * @param {string} cfgUuidStr
 * @returns {Promise<{hostname: string, port: number, data: Uint8Array, resp: Uint8Array}>}
 */
async function read_vless_header(reader, cfgUuidStr) {
    let header = new Uint8Array();
    let readOffset = 0;

    async function inner_read_until(targetOffset) {
        const lenToRead = targetOffset - readOffset;
        if (lenToRead <= 0) return;

        const { value, done } = await read_atleast(reader, lenToRead);
        if (done && value.length < lenToRead) {
            throw new Error('header length too short or stream ended prematurely');
        }
        header = concat_typed_arrays(header, value);
        readOffset += value.length;
    }

    // Version (1 byte), UUID (16 bytes), Packet Length (1 byte)
    await inner_read_until(1 + 16 + 1);

    const version = header[0];
    const uuid = header.slice(1, 17);
    const cfg_uuid = parse_uuid(cfgUuidStr);

    if (!validate_uuid(uuid, cfg_uuid)) {
        throw new Error(`invalid UUID: ${Array.from(uuid).map(b => b.toString(16).padStart(2, '0')).join('')}`);
    }

    const pb_len = header[17]; // Protocol Buffers Length
    const cmdOffset = 1 + 16 + 1 + pb_len; // Command Type Offset
    await inner_read_until(cmdOffset + 1 + 2 + 1); // Command, Port, Address Type

    const cmd = header[cmdOffset];
    const COMMAND_TYPE_TCP = 1;
    if (cmd !== COMMAND_TYPE_TCP) {
        throw new Error(`unsupported command type: ${cmd}`);
    }

    const portOffset = cmdOffset + 1;
    const port = (header[portOffset] << 8) + header[portOffset + 1];
    const atype = header[portOffset + 2]; // Address Type

    let addressEndOffset = -1;
    const ADDRESS_TYPE_IPV4 = 1;
    const ADDRESS_TYPE_STRING = 2;
    const ADDRESS_TYPE_IPV6 = 3;

    const addressStartOffset = portOffset + 3;

    if (atype === ADDRESS_TYPE_IPV4) {
        addressEndOffset = addressStartOffset + 4;
    } else if (atype === ADDRESS_TYPE_IPV6) {
        addressEndOffset = addressStartOffset + 16;
    } else if (atype === ADDRESS_TYPE_STRING) {
        // String address includes a length byte followed by the string
        await inner_read_until(addressStartOffset + 1); // Read string length byte
        const strLen = header[addressStartOffset];
        addressEndOffset = addressStartOffset + 1 + strLen;
    } else {
        throw new Error(`unsupported address type: ${atype}`);
    }

    await inner_read_until(addressEndOffset);

    let hostname = '';
    if (atype === ADDRESS_TYPE_IPV4) {
        hostname = header.slice(addressStartOffset, addressEndOffset).join('.');
    } else if (atype === ADDRESS_TYPE_STRING) {
        const strLen = header[addressStartOffset];
        hostname = new TextDecoder().decode(
            header.slice(addressStartOffset + 1, addressStartOffset + 1 + strLen)
        );
    } else if (atype === ADDRESS_TYPE_IPV6) {
        hostname = header
            .slice(addressStartOffset, addressEndOffset)
            .reduce(
                (s, b2, i2, a) =>
                    i2 % 2 ? s.concat(((a[i2 - 1] << 8) + b2).toString(16)) : s,
                [],
            )
            .join(':');
    }

    if (!hostname) {
        log('error', 'Failed to parse hostname from VLESS header.');
        throw new Error('parse hostname failed');
    }

    log('info', `VLESS connection to ${hostname}:${port}`);
    return {
        hostname,
        port,
        data: header.slice(addressEndOffset), // Remaining data after header
        resp: new Uint8Array([version, 0]), // VLESS response: version + success
    };
}

/**
 * Jiexi VLESS tou bu de zhu yao han shu
 * @param {string} uuidStr
 * @param {{readable: ReadableStream, writable: WritableStream}} clientStream
 * @returns {Promise<object>}
 */
async function parse_header(uuidStr, clientStream) {
    log('debug', 'Starting to parse VLESS header');
    const reader = clientStream.readable.getReader();
    try {
        const vless = await read_vless_header(reader, uuidStr);
        log('debug', 'VLESS header parsed successfully');
        return vless;
    } catch (err) {
        log('error', `VLESS header parse error: ${err.message}`);
        throw new Error(`read vless header error: ${err.message}`);
    } finally {
        reader.releaseLock();
    }
}

/**
 * Lian jie yuan cheng zhu ji, dai you chao shi she zhi
 * @param {string} hostname
 * @param {number} port
 * @param {number} ms - Chao shi shi jian (hao miao)
 * @returns {Promise<net.Socket>}
 */
function timed_connect(hostname, port, ms) {
    return new Promise((resolve, reject) => {
        const conn = net.createConnection({ host: hostname, port: port });
        const timeoutHandle = setTimeout(() => {
            conn.destroy(new Error(`connect timeout to ${hostname}:${port}`));
            reject(new Error(`connect timeout to ${hostname}:${port}`));
        }, ms);

        conn.on('connect', () => {
            clearTimeout(timeoutHandle);
            resolve(conn);
        });
        conn.on('error', (err) => {
            clearTimeout(timeoutHandle);
            reject(new Error(`connect error to ${hostname}:${port}: ${err.message}`));
        });
        conn.on('close', () => {
            clearTimeout(timeoutHandle); // Qing chu chao shi qi, fang zhi zai ci chu fa
        });
    });
}

/**
 * Jian li yuan cheng TCP lian jie
 * @param {string} hostname
 * @param {number} port
 * @returns {Promise<net.Socket>}
 */
async function connect_remote(hostname, port) {
    const timeout = 8000; // 8 miao chao shi
    try {
        const conn = await timed_connect(hostname, port, timeout);

        conn.setNoDelay(SETTINGS.TCP_NODELAY);
        conn.setKeepAlive(SETTINGS.TCP_KEEPALIVE, 1000); // Mei ge 1 miao发送 Keep-Alive bao

        // conn.bufferSize = parseInt(SETTINGS.BUFFER_SIZE) * 1024; // Zai Node.js zhong, zhe ge bu shi tong yong de shu xing

        log('info', `Successfully connected to remote ${hostname}:${port}`);
        return conn;
    } catch (err) {
        log('error', `Failed to connect to remote ${hostname}:${port}: ${err.message}`);
        throw err;
    }
}

/**
 * Jian li shu ju zhong ji yong yu liang ge liu zhi jian de chuan shu
 * @returns {(src: ReadableStream | net.Socket, dest: WritableStream | net.Socket, first_packet: Uint8Array) => Promise<void>}
 */
function pipe_relay() {
    /**
     * Jiang shu ju cong yuan liu chuan shu dao mu biao liu
     * @param {ReadableStream | net.Socket} src - Yuan liu
     * @param {WritableStream | net.Socket} dest - Mu biao liu
     * @param {Uint8Array} first_packet - Di yi ge yao fa song de shu ju bao
     */
    async function pump(src, dest, first_packet) {
        const chunkSize = SETTINGS.CHUNK_SIZE;

        if (first_packet && first_packet.length > 0) {
            if (dest instanceof net.Socket) {
                // Wei Socket jin xing xie ru you hua
                dest.cork(); // Kai qi软木塞 mo shi, jiang duo ge xie ru cao zuo da bao
                dest.write(first_packet);
                process.nextTick(() => dest.uncork()); // Zai xia yi ge shi jian xun huan kai qi软木塞, yi fa song suo you da bao de shu ju
            } else {
                const writer = dest.writable.getWriter();
                try {
                    await writer.write(first_packet);
                } finally {
                    writer.releaseLock();
                }
            }
        }

        try {
            if (src instanceof net.Socket && dest instanceof net.Socket) {
                // You hua Socket dao Socket de guan dao
                src.pipe(dest, { end: true });
            } else if (src.readable && dest.writable) { // Web Streams
                await src.readable.pipeTo(dest.writable, {
                    preventClose: false,
                    preventAbort: false,
                    preventCancel: false,
                    signal: AbortSignal.timeout(SETTINGS.SESSION_TIMEOUT) // She zhi chao shi xin hao
                });
            } else {
                throw new Error('Unsupported stream types for piping');
            }
        } catch (err) {
            // Hu lue yi xie tong chang de lian jie guan bi cuo wu
            if (!err.message.includes('aborted') && !err.message.includes('socket hang up') &&
                !err.message.includes('ERR_STREAM_PREMATURE_CLOSE')) {
                log('error', 'Relay pump error:', err.message);
            }
            throw err; // Ji xu pao chu cuo wu yi chu fa wai bu qing li
        }
    }
    return pump;
}

/**
 * Jiang Node.js Socket zhuan huan wei Web Streams API de ke du/ke xie liu
 * @param {net.Socket} socket
 * @returns {{readable: ReadableStream, writable: WritableStream}}
 */
function socketToWebStream(socket) {
    let readController;
    let writeController;

    const readable = new ReadableStream({
        start(controller) {
            readController = controller;
            socket.on('data', (chunk) => {
                try {
                    controller.enqueue(chunk);
                } catch (err) {
                    log('error', 'Error enqueuing data to readable stream:', err.message);
                    // Bi mian zai yi jing guan bi de liu shang enqueuing
                    if (!controller.desiredSize) { // desiredSize === null yi wei liu yi jing guan bi
                        socket.destroy(); // Guan bi socket
                    }
                }
            });
            socket.on('end', () => {
                log('debug', 'Socket readable ended.');
                try {
                    controller.close();
                } catch (err) {
                    log('error', 'Error closing readable stream controller:', err.message);
                }
            });
            socket.on('error', (err) => {
                log('error', 'Socket error on readable stream:', err.message);
                controller.error(err);
            });
        },
        cancel(reason) {
            log('debug', 'Readable stream cancelled, destroying socket.', reason);
            socket.destroy(reason);
        }
    });

    const writable = new WritableStream({
        start(controller) {
            writeController = controller;
            socket.on('error', (err) => { // Zai xie ru liu shang jian ting socket cuo wu
                log('error', 'Socket error on writable stream:', err.message);
                controller.error(err);
            });
        },
        async write(chunk) {
            return new Promise((resolve, reject) => {
                if (socket.destroyed || socket.writableEnded) {
                    // log('warn', 'Attempted to write to a destroyed or ended socket.');
                    return reject(new Error('Socket is destroyed or already ended.'));
                }
                socket.write(chunk, (err) => {
                    if (err) {
                        log('error', 'Socket write error:', err.message);
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        },
        close() {
            log('debug', 'Writable stream closed, ending socket.');
            if (!socket.destroyed) {
                socket.end(); // Guan bi socket de xie ru duan
            }
        },
        abort(reason) {
            log('warn', 'Writable stream aborted, destroying socket.', reason);
            socket.destroy(reason);
        }
    });

    return { readable, writable };
}


/**
 * Chu li shu ju zhong ji guo cheng
 * @param {object} cfg - Peizhi dui xiang (bu shi yong)
 * @param {{readable: ReadableStream, writable: WritableStream, reading_done?: Function}} client - Ke hu duan liu
 * @param {net.Socket} remote - Yuan cheng socket
 * @param {{hostname: string, port: number, data: Uint8Array, resp: Uint8Array}} vless - VLESS tou bu xin xi
 */
function relay(cfg, client, remote, vless) {
    const pump = pipe_relay();
    let isClosing = false; // Yong yu fang zhi duo ci qing li

    const remoteStream = socketToWebStream(remote);

    const cleanup = () => {
        if (!isClosing) {
            isClosing = true;
            log('debug', 'Initiating relay cleanup.');
            try {
                if (remote && !remote.destroyed) {
                    remote.destroy(); // Que bao yuan cheng socket bei guan bi
                    log('debug', 'Remote socket destroyed during cleanup.');
                }
            } catch (err) {
                // Hu lue yi xie chang jian de guan bi cuo wu
                if (!err.message.includes('aborted') &&
                    !err.message.includes('socket hang up') &&
                    !err.message.includes('ERR_STREAM_PREMATURE_CLOSE')) {
                    log('error', `Error during relay cleanup: ${err.message}`);
                }
            }
        }
    };

    // Ke hu duan shang chuan shu ju dao yuan cheng
    const uploader = pump(client, remoteStream, vless.data)
        .catch(err => {
            if (!err.message.includes('aborted') && !err.message.includes('socket hang up') &&
                !err.message.includes('ERR_STREAM_PREMATURE_CLOSE')) {
                log('error', `Data upload error: ${err.message}`);
            }
        })
        .finally(() => {
            // log('debug', 'Uploader finished or errored.');
            if (client.reading_done) {
                client.reading_done(); // Ruo you xuan ze de hui diao han shu
            }
            cleanup(); // Guan bi shang chuan hou qing li
        });

    // Yuan cheng xia zai shu ju dao ke hu duan
    const downloader = pump(remoteStream, client, vless.resp)
        .catch(err => {
            if (!err.message.includes('aborted') && !err.message.includes('socket hang up') &&
                !err.message.includes('ERR_STREAM_PREMATURE_CLOSE')) {
                log('error', `Data download error: ${err.message}`);
            }
        })
        .finally(() => {
            // log('debug', 'Downloader finished or errored.');
            cleanup(); // Guan bi xia zai hou qing li
        });

    // Lian ge fang xiang du jie shu huo fa sheng cuo wu shi jin xing qing li
    Promise.allSettled([uploader, downloader])
        .finally(() => {
            log('debug', 'Both relay streams settled, final cleanup.');
            cleanup();
        });
}

/**
 * Hui hua guan li lei
 */
class Session {
    constructor(uuid) {
        this.uuid = uuid;
        this.nextSeq = 0; // Xia yi ge qi dai de shu ju bao xu hao
        this.downstreamStarted = false; // Biao zhi xia zai liu shi fou yi jing kai shi
        this.lastActivity = Date.now(); // Zui hou huo dong shi jian
        this.vlessHeader = null; // Chu cun jie xi hou de VLESS tou bu xin xi
        this.remote = null; // Yuan cheng TCP Socket
        this.initialized = false; // Biao zhi VLESS lian jie shi fou yi chu shi hua
        this.responseHeader = null; // VLESS xiang ying tou bu
        this.headerSent = false; // Biao zhi xiang ying tou bu shi fou yi jing fa song
        this.pendingBuffers = new Map(); // Cun chu wu xu da bao
        this.cleaned = false; // Biao zhi hui hua shi fou yi jing qing li
        this.currentStreamRes = null; // Dang qian zheng zai shi yong de HTTP xiang ying dui xiang (ke hu duan xia zai liu)
        log('debug', `Created new session with UUID: ${uuid}`);
    }

    /**
     * Chu shi hua VLESS lian jie he yuan cheng lian jie
     * @param {Buffer} firstPacket - Di yi ge shu ju bao, yong yu jie xi VLESS tou bu
     * @returns {Promise<boolean>} - Chu shi hua cheng gong fan hui true, fou ze fan hui false
     */
    async initializeVLESS(firstPacket) {
        if (this.initialized) return true;

        log('debug', 'Initializing VLESS connection from first packet');
        const readable = new ReadableStream({
            start(controller) {
                controller.enqueue(firstPacket);
                controller.close();
            }
        });

        const clientPseudoStream = {
            readable: readable,
            writable: new WritableStream() // Bu shi yong, jin wei jie kou bi mian cuo wu
        };

        try {
            this.vlessHeader = await parse_header(SETTINGS.UUID, clientPseudoStream);
            log('info', `VLESS header parsed for session ${this.uuid}: ${this.vlessHeader.hostname}:${this.vlessHeader.port}`);

            this.remote = await connect_remote(this.vlessHeader.hostname, this.vlessHeader.port);
            log('info', `Remote connection established for session ${this.uuid}`);

            this.initialized = true;
            this.responseHeader = Buffer.from(this.vlessHeader.resp); // Chu cun VLESS xiang ying tou bu
            await this._writeToRemote(this.vlessHeader.data); // Jiang VLESS tou bu hou mian de shu ju xie ru yuan cheng

            // Ruo xia zai liu yi jing zhun bei hao, ze kai shi xiang ying
            if (this.currentStreamRes) {
                this._startDownstreamResponse();
            }
            return true;
        } catch (err) {
            log('error', `Failed to initialize VLESS for session ${this.uuid}: ${err.message}`);
            this.cleanup(); // Chu shi hua shi bai ze qing li hui hua
            return false;
        }
    }

    /**
     * Chu li shang chuan de shu ju bao (ru he cai yong POST fang shi)
     * @param {number} seq - Shu ju bao xu hao
     * @param {Buffer} data - Shu ju bao shu ju
     * @returns {Promise<boolean>} - Chu li cheng gong fan hui true
     */
    async processPacket(seq, data) {
        this.lastActivity = Date.now(); // Geng xin zui hou huo dong shi jian
        try {
            this.pendingBuffers.set(seq, data);
            log('debug', `Buffered packet seq=${seq}, size=${data.length} for session ${this.uuid}`);

            while (this.pendingBuffers.has(this.nextSeq)) {
                const nextData = this.pendingBuffers.get(this.nextSeq);
                this.pendingBuffers.delete(this.nextSeq);

                if (!this.initialized) {
                    // Di yi ge bao (xu hao 0) yong yu chu shi hua VLESS
                    if (this.nextSeq === 0) {
                        if (!await this.initializeVLESS(nextData)) {
                            throw new Error('Failed to initialize VLESS connection with first packet.');
                        }
                    } else {
                        // Ruo zai chu shi hua qian shou dao fei 0 xu hao de bao, ke neng shi cuo wu qing kuang
                        log('warn', `Received out of order packet seq=${this.nextSeq} before initialization for session ${this.uuid}. Data dropped.`);
                        this.cleanup(); // Chu xian yi chang qing li
                        throw new Error('Out of order packet before session initialization.');
                    }
                } else {
                    // Yi jing chu shi hua, zhi jie xie ru yuan cheng
                    await this._writeToRemote(nextData);
                }
                this.nextSeq++;
                log('debug', `Processed packet seq=${this.nextSeq - 1} for session ${this.uuid}.`);
            }

            if (this.pendingBuffers.size > SETTINGS.MAX_BUFFERED_POSTS) {
                log('error', `Too many buffered packets for session ${this.uuid}. Max: ${SETTINGS.MAX_BUFFERED_POSTS}`);
                this.cleanup(); // Chao guo huan chong xian zhi qing li
                throw new Error('Too many buffered packets, session terminated.');
            }
            return true;
        } catch (err) {
            log('error', `Error processing packet seq=${seq} for session ${this.uuid}: ${err.message}`);
            this.cleanup();
            throw err;
        }
    }

    /**
     * Kai shi jiang shu ju chuan shu gei ke hu duan (xia zai liu)
     * @param {http.ServerResponse} res - HTTP xiang ying dui xiang
     * @param {object} headers - Xiang ying tou bu
     * @returns {boolean} - Chu li cheng gong fan hui true
     */
    startDownstream(res, headers) {
        if (this.currentStreamRes) {
            log('warn', `Downstream already started for session ${this.uuid}. New request ignored.`);
            return false; // Fang zhi zhong fu kai shi
        }

        if (!res.headersSent) {
            res.writeHead(200, headers);
        }

        this.currentStreamRes = res;
        this.downstreamStarted = true;
        this.lastActivity = Date.now(); // Geng xin zui hou huo dong shi jian

        // Ruo yi jing chu shi hua qie you VLESS xiang ying tou bu, ze zhi jie kai shi chuan shu
        if (this.initialized && this.responseHeader) {
            this._startDownstreamResponse();
        }

        // Jian ting ke hu duan lian jie guan bi shi jian
        res.on('close', () => {
            log('info', `Client connection closed for session ${this.uuid}`);
            this.cleanup();
            sessions.delete(this.uuid); // Cong quan ju map zhong yi chu
        });
        res.on('error', (err) => {
            log('error', `Client response stream error for session ${this.uuid}: ${err.message}`);
            this.cleanup();
            sessions.delete(this.uuid);
        });
        return true;
    }

    /**
     * Jiang shu ju xie ru yuan cheng socket
     * @param {Buffer} data - Yao xie ru de shu ju
     * @returns {Promise<void>}
     */
    async _writeToRemote(data) {
        if (!this.remote || this.remote.destroyed) {
            const msg = `Remote connection not available or destroyed for session ${this.uuid}`;
            log('error', msg);
            throw new Error(msg);
        }

        return new Promise((resolve, reject) => {
            this.remote.write(data, (err) => {
                if (err) {
                    log('error', `Failed to write ${data.length} bytes to remote for session ${this.uuid}: ${err.message}`);
                    reject(err);
                } else {
                    // log('debug', `Successfully wrote ${data.length} bytes to remote for session ${this.uuid}.`);
                    resolve();
                }
            });
        });
    }

    /**
     * Kai shi jiang yuan cheng socket de shu ju liu xiang ke hu duan xiang ying
     */
    _startDownstreamResponse() {
        if (!this.currentStreamRes || !this.remote || this.headerSent) {
            log('warn', `Attempted to start downstream response for session ${this.uuid} but conditions not met.`);
            return;
        }

        try {
            // Xian fa song VLESS xiang ying tou bu
            this.currentStreamRes.write(this.responseHeader);
            this.headerSent = true;
            log('debug', `Sent VLESS response header for session ${this.uuid}.`);

            // Jiang yuan cheng socket guan dao ke hu duan xiang ying liu
            this.remote.pipe(this.currentStreamRes);

            this.remote.on('end', () => {
                log('info', `Remote socket ended for session ${this.uuid}.`);
                if (!this.currentStreamRes.writableEnded) {
                    this.currentStreamRes.end(); // Que bao ke hu duan xiang ying ye jie shu
                }
            });

            this.remote.on('error', (err) => {
                log('error', `Remote socket error for session ${this.uuid}: ${err.message}`);
                if (!this.currentStreamRes.writableEnded) {
                    this.currentStreamRes.end(); // Cuo wu shi guan bi xiang ying
                }
                this.cleanup();
            });

        } catch (err) {
            log('error', `Error initiating downstream response for session ${this.uuid}: ${err.message}`);
            this.cleanup();
        }
    }

    /**
     * Qing li hui hua zi yuan
     */
    cleanup() {
        if (!this.cleaned) {
            this.cleaned = true;
            log('info', `Cleaning up session ${this.uuid}`);
            if (this.remote) {
                this.remote.destroy(); // Guan bi yuan cheng socket
                this.remote = null;
            }
            // Qing kong dai chu li de shu ju
            this.pendingBuffers.clear();
            this.initialized = false;
            this.headerSent = false;
            this.currentStreamRes = null; // Shan chu dui xiang ying de yin yong
            log('debug', `Session ${this.uuid} cleaned up.`);
        }
    }
}

const sessions = new Map(); // Cun chu huo yue hui hua

// Huo qu ying she xin xi
const metaInfo = execSync(
    'curl -s https://speed.cloudflare.com/meta | awk -F\\" \'{print $26"-"$18}\' | sed -e \'s/ /_/g\'',
    { encoding: 'utf-8' }
).trim();
const ISP = metaInfo;
let IP = DOMAIN; // Shou xian shi yong DOMAIN
if (!DOMAIN) { // Ruo DOMAIN bu cun zai, zai chang shi huo qu gong wang IP
    try {
        IP = execSync('curl -s --max-time 2 ipv4.ip.sb', { encoding: 'utf-8' }).trim();
    } catch (ipv4Err) {
        log('warn', 'Failed to get IPv4 address, trying IPv6:', ipv4Err.message);
        try {
            IP = `[${execSync('curl -s --max-time 1 ipv6.ip.sb', { encoding: 'utf-8' }).trim()}]`; // IPv6 jia shang fang kuo hao
        } catch (ipv6Err) {
            log('error', 'Failed to get IP address from both IPv4 and IPv6 services:', ipv6Err.message);
            IP = 'localhost'; // Bei yong
        }
    }
}

// Ding yi chu li hui hua chao shi de ji shi qi
function setupSessionCleanupTimer() {
    setInterval(() => {
        const now = Date.now();
        for (const [uuid, session] of sessions.entries()) {
            // Ruo hui hua chao shi qie mei you xia zai liu kai shi, huo zhe liu yi chang shi jian bu huo yue
            if ((!session.downstreamStarted && (now - session.lastActivity > SETTINGS.SESSION_TIMEOUT)) ||
                (session.downstreamStarted && (now - session.lastActivity > SETTINGS.SESSION_TIMEOUT * 2))) { // Xia zai liu ke yi shi dang yan chang chao shi
                log('warn', `Session ${uuid} timed out or inactive, cleaning up.`);
                session.cleanup();
                sessions.delete(uuid);
            }
        }
    }, SETTINGS.SESSION_TIMEOUT / 2); // Mei ge yi ding shi jian jian cha
}


const server = http.createServer((req, res) => {
    // She zhi chang yong de HTTP tou bu
    const commonHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', // Zeng jia OPTIONS yi zhi chi CORS yu jian fei qiu
        'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate', // Geng qiang de huan cun kong zhi
        'Pragma': 'no-cache',
        'Expires': '0',
        'X-Accel-Buffering': 'no', // Guan bi Nginx yuan duan bu chong
        'X-Padding': generatePadding(100, 1000), // Zeng jia sui ji tian chong
        'Content-Security-Policy': "default-src 'self'", // Ji ben an quan tou
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'Referrer-Policy': 'no-referrer-when-downgrade',
    };

    // Chu li OPTIONS yu jian fei qiu
    if (req.method === 'OPTIONS') {
        res.writeHead(204, commonHeaders); // 204 No Content
        res.end();
        return;
    }

    // Lu you: Gen mu lu
    if (req.url === '/') {
        res.writeHead(200, { ...commonHeaders, 'Content-Type': 'text/plain' });
        res.end('Hello, World\n');
        return;
    }

    // Lu you: Ding yue lu jing
    if (req.url === `/${SUB_PATH}`) {
        const vlessURL = `vless://${UUID}@${IP}:443?encryption=none&security=tls&sni=${IP}&fp=chrome&allowInsecure=1&type=xhttp&host=${IP}&path=${SETTINGS.XPATH}&mode=packet-up#${NAME}-${ISP}`;
        const base64Content = Buffer.from(vlessURL).toString('base64');
        res.writeHead(200, { ...commonHeaders, 'Content-Type': 'text/plain' });
        res.end(base64Content + '\n');
        return;
    }

    // Lu you: VLESS shu ju chuan shu lu jing
    const pathMatch = req.url.match(new RegExp(`${XPATH}/([^/]+)(?:/([0-9]+))?$`));
    if (!pathMatch) {
        log('warn', `Invalid URL path: ${req.url}`);
        res.writeHead(404, commonHeaders);
        res.end('Not Found');
        return;
    }

    const uuid = pathMatch[1];
    const seq = pathMatch[2] ? parseInt(pathMatch[2]) : null;

    // Chu li GET qing qiu (xia zai liu)
    if (req.method === 'GET' && seq === null) { // GET qing qiu tong chang bu dai seq
        const headersForGet = {
            ...commonHeaders,
            'Content-Type': 'application/octet-stream',
            'Transfer-Encoding': 'chunked',
            // 'Connection': 'keep-alive', // Zai HTTP/1.1 xia guan bi Keep-Alive yi shi xian shi shi liu
        };

        let session = sessions.get(uuid);
        if (!session) {
            session = new Session(uuid);
            sessions.set(uuid, session);
            log('info', `Created new session for GET (downstream): ${uuid}`);
        } else {
            log('info', `Reusing existing session for GET (downstream): ${uuid}`);
        }

        if (!session.startDownstream(res, headersForGet)) {
            log('error', `Failed to start downstream for session: ${uuid}`);
            if (!res.headersSent) {
                res.writeHead(500, commonHeaders);
                res.end('Internal Server Error');
            }
            session.cleanup();
            sessions.delete(uuid);
        }
        return;
    }

    // Chu li POST qing qiu (shang chuan liu)
    if (req.method === 'POST' && seq !== null) {
        let session = sessions.get(uuid);
        if (!session) {
            session = new Session(uuid);
            sessions.set(uuid, session);
            log('info', `Created new session for POST (upstream): ${uuid}`);
        } else {
            log('info', `Reusing existing session for POST (upstream): ${uuid}`);
        }

        let requestDataChunks = [];
        let totalSize = 0;
        let headersSent = false;

        req.on('data', chunk => {
            totalSize += chunk.length;
            if (totalSize > SETTINGS.MAX_POST_SIZE) {
                if (!headersSent) {
                    res.writeHead(413, commonHeaders); // 413 Payload Too Large
                    res.end('Payload Too Large');
                    headersSent = true;
                }
                req.destroy(); // Ting zhi jie shou shu ju
                return;
            }
            requestDataChunks.push(chunk);
        });

        req.on('end', async () => {
            if (headersSent) return;

            try {
                const buffer = Buffer.concat(requestDataChunks);
                log('info', `Received POST packet: seq=${seq}, size=${buffer.length} for session ${uuid}`);

                await session.processPacket(seq, buffer);

                if (!headersSent) {
                    res.writeHead(200, commonHeaders);
                    res.end('OK');
                    headersSent = true;
                }

            } catch (err) {
                log('error', `Failed to process POST request for session ${uuid}: ${err.message}`);
                session.cleanup();
                sessions.delete(uuid);

                if (!headersSent) {
                    res.writeHead(500, commonHeaders);
                    res.end('Internal Server Error');
                    headersSent = true;
                }
            }
        });

        req.on('error', (err) => {
            log('error', `Request stream error for session ${uuid}: ${err.message}`);
            if (!headersSent) {
                res.writeHead(500, commonHeaders);
                res.end('Internal Server Error');
                headersSent = true;
            }
            session.cleanup();
            sessions.delete(uuid);
        });
        return;
    }

    // Qi ta bu fu he gui ze de qing qiu
    log('warn', `Unhandled request method/path: ${req.method} ${req.url}`);
    res.writeHead(404, commonHeaders);
    res.end('Not Found');
});

server.on('secureConnection', (socket) => {
    log('debug', `New secure connection established. ALPN: ${socket.alpnProtocol || 'http/1.1'}`);
});

/**
 * Sheng cheng sui ji tian chong zi fu chuan
 * @param {number} min - Zui xiao chang du
 * @param {number} max - Zui da chang du
 * @returns {string} - Base64 bian ma de tian chong zi fu chuan
 */
function generatePadding(min, max) {
    const length = min + Math.floor(Math.random() * (max - min + 1));
    // Shi yong Math.random() lai sheng cheng tian chong nei rong, er bu shi 'X'
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return Buffer.from(result).toString('base64');
}

// Zeng da Keep-Alive chao shi, ti gao lian jie fu yong xing
server.keepAliveTimeout = 620 * 1000; // 10 fen zhong
server.headersTimeout = 625 * 1000;   // Bi Keep-Alive chao shi shao chang yi xie

server.on('error', (err) => {
    log('error', `HTTP Server error: ${err.message}`);
});

server.listen(PORT, () => {
    log('info', `Server is listening on port ${PORT}`);
    setupSessionCleanupTimer(); // Qi dong hui hua qing li ji shi qi
});
