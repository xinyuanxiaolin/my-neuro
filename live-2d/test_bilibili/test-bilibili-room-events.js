const zlib = require('zlib');
const crypto = require('crypto');
const ws = require('ws');

const roomIdInput = Number(process.argv[2] || 22381731);
const preferredHost = process.argv[3] || '';

const EVENT_COMMANDS = new Set([
    'SEND_GIFT',
    'COMBO_SEND',
    'GUARD_BUY',
    'SUPER_CHAT_MESSAGE',
    'SUPER_CHAT_MESSAGE_JPN',
    'INTERACT_WORD',
    'NOTICE_MSG',
    'ENTRY_EFFECT',
    'WELCOME',
    'WELCOME_GUARD'
]);

async function main() {
    console.log(`[TEST] 输入房间号: ${roomIdInput}`);

    const roomInfo = await fetchJson(
        `https://api.live.bilibili.com/room/v1/Room/room_init?id=${roomIdInput}`
    );
    const realRoomId = roomInfo?.data?.room_id;

    if (!realRoomId) {
        throw new Error(`无法获取真实房间号: ${JSON.stringify(roomInfo)}`);
    }

    console.log(`[TEST] 真实房间号: ${realRoomId}`);

    const danmuInfo = await fetchDanmuInfo(realRoomId);

    if (danmuInfo?.code !== 0) {
        throw new Error(`getDanmuInfo 失败: ${JSON.stringify(danmuInfo)}`);
    }

    const hostList = danmuInfo?.data?.host_list || [];
    const token = danmuInfo?.data?.token;
    if (!token || hostList.length === 0) {
        throw new Error(`无法获取弹幕服务器或 token: ${JSON.stringify(danmuInfo)}`);
    }

    const host =
        hostList.find((item) => preferredHost && item.host === preferredHost) ||
        hostList[0];
    const port = host.wss_port || host.ws_port || 443;
    const url = `wss://${host.host}:${port}/sub`;

    console.log(`[TEST] WebSocket: ${url}`);
    console.log(`[TEST] token: ${token.slice(0, 12)}...`);

    const client = new ws(url, { perMessageDeflate: false });
    let heartbeatTimer = null;

    client.on('open', () => {
        console.log('[TEST] WebSocket 已连接，发送鉴权包');
        const authBody = {
            uid: 0,
            roomid: realRoomId,
            protover: 3,
            platform: 'web',
            clientver: '1.18.5',
            type: 2,
            key: token
        };

        client.send(buildPacket(authBody, 7, 1));
        heartbeatTimer = setInterval(() => {
            const heartbeatBody = {
                roomid: realRoomId,
                protover: 3,
                platform: 'web',
                type: 2
            };
            client.send(buildPacket(heartbeatBody, 2, 1));
        }, 30000);
    });

    client.on('message', (raw) => {
        try {
            for (const payload of decodePacket(raw)) {
                if (!payload || typeof payload !== 'object') continue;
                const cmd = String(payload.cmd || payload.command || '').split(':')[0];
                if (!cmd) continue;

                if (cmd === 'ONLINE_RANK_COUNT' || cmd === 'WATCHED_CHANGE') {
                    continue;
                }

                if (EVENT_COMMANDS.has(cmd)) {
                    console.log(`[EVENT] ${cmd}`);
                    console.log(JSON.stringify(summarizePayload(cmd, payload), null, 2));
                } else if (cmd === 'DANMU_MSG') {
                    const summary = summarizeDanmu(payload);
                    if (summary) {
                        console.log(`[DANMU] ${summary.nickname}: ${summary.text}`);
                    }
                }
            }
        } catch (error) {
            console.error('[TEST] 消息处理失败:', error);
        }
    });

    client.on('close', (code, reason) => {
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        console.log(`[TEST] WebSocket 已关闭: code=${code} reason=${reason?.toString?.() || ''}`);
    });

    client.on('error', (error) => {
        console.error('[TEST] WebSocket 错误:', error);
    });
}

async function fetchJson(url) {
    const response = await fetch(url, {
        headers: {
            'User-Agent': 'Mozilla/5.0'
        }
    });

    if (!response.ok) {
        throw new Error(`HTTP ${response.status} ${response.statusText} @ ${url}`);
    }

    return response.json();
}

async function fetchDanmuInfo(realRoomId) {
    const nav = await fetchJsonWithHeaders(
        'https://api.bilibili.com/x/web-interface/nav',
        {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.bilibili.com/'
        }
    );

    const imgUrl = nav?.data?.wbi_img?.img_url;
    const subUrl = nav?.data?.wbi_img?.sub_url;
    if (!imgUrl || !subUrl) {
        throw new Error(`无法获取 WBI 签名素材: ${JSON.stringify(nav)}`);
    }

    const imgKey = imgUrl.split('/').pop().split('.')[0];
    const subKey = subUrl.split('/').pop().split('.')[0];
    const query = encodeWbiQuery({ id: realRoomId, type: 0 }, imgKey, subKey);
    const url = `https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?${query}`;

    return fetchJsonWithHeaders(url, {
        'User-Agent': 'Mozilla/5.0',
        'Referer': `https://live.bilibili.com/${realRoomId}`,
        'Origin': 'https://live.bilibili.com'
    });
}

async function fetchJsonWithHeaders(url, headers) {
    const response = await fetch(url, { headers });

    if (!response.ok) {
        throw new Error(`HTTP ${response.status} ${response.statusText} @ ${url}`);
    }

    return response.json();
}

function encodeWbiQuery(params, imgKey, subKey) {
    const mixinKey = getMixinKey(imgKey + subKey);
    const chrFilter = /[!'()*]/g;
    const wts = Math.round(Date.now() / 1000);
    const search = new URLSearchParams();

    Object.entries({ ...params, wts })
        .sort((a, b) => a[0].localeCompare(b[0]))
        .forEach(([key, value]) => {
            search.append(key, String(value).replace(chrFilter, ''));
        });

    const query = search.toString().replace(/\+/g, '%20');
    const wRid = crypto.createHash('md5').update(query + mixinKey).digest('hex');
    return `${query}&w_rid=${wRid}`;
}

function getMixinKey(origin) {
    const mixinKeyEncTab = [
        46, 47, 18, 2, 53, 8, 23, 32,
        15, 50, 10, 31, 58, 3, 45, 35,
        27, 43, 5, 49, 33, 9, 42, 19,
        29, 28, 14, 39, 12, 38, 41, 13,
        37, 48, 7, 16, 24, 55, 40, 61,
        26, 17, 0, 1, 60, 51, 30, 4,
        22, 25, 54, 21, 56, 59, 6, 63,
        57, 62, 11, 36, 20, 34, 44, 52
    ];

    return mixinKeyEncTab.map((index) => origin[index]).join('').slice(0, 32);
}

function buildPacket(body, operation, version = 1, sequence = 1) {
    const payload = Buffer.isBuffer(body)
        ? body
        : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body), 'utf8');

    const packetLength = 16 + payload.length;
    const packet = Buffer.alloc(packetLength);
    packet.writeInt32BE(packetLength, 0);
    packet.writeInt16BE(16, 4);
    packet.writeInt16BE(version, 6);
    packet.writeInt32BE(operation, 8);
    packet.writeInt32BE(sequence, 12);
    payload.copy(packet, 16);
    return packet;
}

function decodePacket(rawData) {
    const buffer = normalizeBuffer(rawData);
    const results = [];
    parsePacket(buffer, results);
    return results;
}

function parsePacket(buffer, results) {
    let offset = 0;
    while (offset + 16 <= buffer.length) {
        const packetLength = buffer.readInt32BE(offset);
        if (packetLength < 16 || offset + packetLength > buffer.length) break;

        const headerLength = buffer.readInt16BE(offset + 4);
        const version = buffer.readInt16BE(offset + 6);
        const operation = buffer.readInt32BE(offset + 8);
        const body = buffer.slice(offset + headerLength, offset + packetLength);

        if (operation === 5) {
            if (version === 2) {
                parsePacket(zlib.inflateSync(body), results);
            } else if (version === 3) {
                parsePacket(zlib.brotliDecompressSync(body), results);
            } else {
                const payload = decodeJson(body);
                if (payload) results.push(payload);
            }
        } else if (operation === 8) {
            console.log('[TEST] 鉴权成功');
        } else if (operation === 3 && body.length >= 4) {
            console.log(`[TEST] 人气值: ${body.readUInt32BE(0)}`);
        }

        offset += packetLength;
    }
}

function normalizeBuffer(rawData) {
    if (Buffer.isBuffer(rawData)) return rawData;
    if (rawData instanceof ArrayBuffer) return Buffer.from(rawData);
    if (Array.isArray(rawData)) return Buffer.concat(rawData.map((item) => Buffer.from(item)));
    return Buffer.from(rawData);
}

function decodeJson(body) {
    const text = body.toString('utf8').replace(/\0+$/g, '').trim();
    if (!text) return null;

    try {
        return JSON.parse(text);
    } catch (error) {
        return null;
    }
}

function summarizePayload(cmd, payload) {
    const data = payload.data || {};

    if (cmd === 'SEND_GIFT' || cmd === 'COMBO_SEND') {
        return {
            cmd,
            uname: data.uname,
            action: data.action,
            giftName: data.giftName || data.gift_name,
            num: data.num || data.combo_num,
            price: data.price,
            total_coin: data.total_coin,
            coin_type: data.coin_type,
            blind_gift: data.blind_gift,
            raw: data
        };
    }

    if (cmd === 'GUARD_BUY') {
        return {
            cmd,
            username: data.username,
            gift_name: data.gift_name,
            guard_level: data.guard_level,
            num: data.num,
            price: data.price,
            raw: data
        };
    }

    if (cmd === 'SUPER_CHAT_MESSAGE' || cmd === 'SUPER_CHAT_MESSAGE_JPN') {
        return {
            cmd,
            uname: data.user_info?.uname || data.uname,
            message: data.message,
            price: data.price,
            time: data.time,
            raw: data
        };
    }

    if (cmd === 'INTERACT_WORD') {
        return {
            cmd,
            uname: data.uname,
            msg_type: data.msg_type,
            fans_medal: data.fans_medal,
            raw: data
        };
    }

    return {
        cmd,
        raw: data
    };
}

function summarizeDanmu(payload) {
    const info = payload.info;
    if (!Array.isArray(info)) return null;

    return {
        text: info[1],
        nickname: info[2]?.[1] || '未知用户'
    };
}

main().catch((error) => {
    console.error('[TEST] 启动失败:', error);
    process.exitCode = 1;
});
