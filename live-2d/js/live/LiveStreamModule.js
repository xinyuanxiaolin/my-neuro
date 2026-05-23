const crypto = require('crypto');
const zlib = require('zlib');
const { logToTerminal } = require('../api-utils.js');

const MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32,
    15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19,
    29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61,
    26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63,
    57, 62, 11, 36, 20, 34, 44, 52
];

class LiveStreamModule {
    constructor(config) {
        this.roomId = config.roomId || '30230160';
        this.checkInterval = config.checkInterval || 5000;
        this.maxMessages = config.maxMessages || 50;
        this.apiUrl = config.apiUrl || 'http://api.live.bilibili.com/ajax/msg';
        this.onNewMessage = config.onNewMessage || null;

        this.onNewGift = config.onNewGift || null;
        this.enableGiftSocket = config.enableGiftSocket === true;
        this.giftReconnectInterval = config.giftReconnectInterval || 5000;
        this.giftHeartbeatInterval = config.giftHeartbeatInterval || 30000;
        this.giftHealthCheckInterval = config.giftHealthCheckInterval || 15000;
        this.giftNoPacketTimeout = config.giftNoPacketTimeout || 90000;
        this.giftSessionMaxAge = config.giftSessionMaxAge || 12 * 60 * 1000;
        this.giftWsDanmuSilenceTimeout = config.giftWsDanmuSilenceTimeout || 120000;

        this.lastCheckedTimestamp = Date.now() / 1000;
        this.isRunning = false;
        this.checkTimer = null;
        this.messageCache = [];

        this._giftWs = null;
        this._giftHeartbeatTimer = null;
        this._giftHealthTimer = null;
        this._giftReconnectTimer = null;
        this._giftShouldReconnect = false;
        this._giftSocketInitializing = false;
        this._giftReconnectOptions = null;

        this._resolvedRoomId = null;
        this._danmuInfo = null;
        this._lastWbiKeys = null;
        this._lastBarrageAt = 0;
        this._giftConnectedAt = 0;
        this._giftLastPacketAt = 0;
        this._giftLastBusinessPacketAt = 0;
        this._giftLastWsDanmuAt = 0;
        this._giftHostIndex = 0;
    }

    start() {
        if (this.isRunning) return false;

        this.isRunning = true;
        this.fetchBarrage();

        this.checkTimer = setInterval(() => {
            this.fetchBarrage();
        }, this.checkInterval);

        if (this.enableGiftSocket) {
            this._connectGiftSocket();
        }

        console.log(`直播模块已启动，监听房间: ${this.roomId}`);
        logToTerminal('info', `[LiveStream] 直播模块已启动 | 房间:${this.roomId} | 礼物WS:${this.enableGiftSocket ? 'on' : 'off'}`);
        return true;
    }

    stop() {
        if (!this.isRunning) return false;

        clearInterval(this.checkTimer);
        this.checkTimer = null;
        this.isRunning = false;

        this._stopGiftSocket();

        console.log('直播模块已停止');
        logToTerminal('info', `[LiveStream] 直播模块已停止 | 房间:${this.roomId}`);
        return true;
    }

    async fetchBarrage() {
        try {
            const url = `${this.apiUrl}?roomid=${this.roomId}`;
            const response = await fetch(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
                }
            });

            if (!response.ok) {
                throw new Error(`获取弹幕失败: HTTP ${response.status}`);
            }

            const data = await response.json();
            if (!data || !data.data || !data.data.room) {
                throw new Error('API返回数据格式错误');
            }

            const messages = data.data.room;
            const newMessages = messages.filter((message) => {
                const messageTime = new Date(message.timeline).getTime() / 1000;
                return messageTime > this.lastCheckedTimestamp;
            });

            if (newMessages.length > 0) {
                this.lastCheckedTimestamp = Date.now() / 1000;
                this._lastBarrageAt = Date.now();
                this.messageCache = [...this.messageCache, ...newMessages];

                if (this.messageCache.length > this.maxMessages) {
                    this.messageCache = this.messageCache.slice(-this.maxMessages);
                }

                for (const message of newMessages) {
                    if (this.onNewMessage) {
                        this.onNewMessage(message);
                    }
                }
            }
        } catch (error) {
            console.error('获取弹幕出错:', error);
        }
    }

    getMessages() {
        return [...this.messageCache];
    }

    clearMessages() {
        this.messageCache = [];
    }

    setRoomId(roomId) {
        if (!roomId) return false;

        this.roomId = roomId;
        this._resolvedRoomId = null;
        this._danmuInfo = null;

        if (this.isRunning) {
            this.stop();
            this.start();
        }

        return true;
    }

    setCheckInterval(interval) {
        if (!interval || interval < 1000) return false;

        this.checkInterval = interval;

        if (this.isRunning) {
            this.stop();
            this.start();
        }

        return true;
    }

    getStatus() {
        return {
            isRunning: this.isRunning,
            roomId: this.roomId,
            resolvedRoomId: this._resolvedRoomId,
            checkInterval: this.checkInterval,
            lastCheckedTimestamp: this.lastCheckedTimestamp,
            messageCount: this.messageCache.length,
            giftSocketEnabled: this.enableGiftSocket,
            giftSocketConnected: !!this._giftWs && this._giftWs.readyState === 1,
            lastBarrageAt: this._lastBarrageAt,
            giftConnectedAt: this._giftConnectedAt,
            giftLastPacketAt: this._giftLastPacketAt,
            giftLastBusinessPacketAt: this._giftLastBusinessPacketAt,
            giftLastWsDanmuAt: this._giftLastWsDanmuAt
        };
    }

    async _connectGiftSocket() {
        if (!this.isRunning || !this.enableGiftSocket || this._giftSocketInitializing) return;
        if (this._giftWs && (this._giftWs.readyState === 0 || this._giftWs.readyState === 1)) return;

        const reconnectOptions = this._giftReconnectOptions || {};
        this._giftReconnectOptions = null;

        let WebSocketImpl = null;
        try {
            WebSocketImpl = require('ws');
        } catch (err) {
            console.warn(`礼物 WebSocket 模块不可用，已跳过：${err.message}`);
            return;
        }

        this._giftShouldReconnect = true;
        this._giftSocketInitializing = true;
        this._clearGiftReconnectTimer();
        logToTerminal(
            'info',
            `[LiveStream] 开始连接礼物WS | 房间:${this.roomId} | 原因:${reconnectOptions.reason || 'initial'}`
        );

        try {
            const danmuInfo = await this._getDanmuInfo({
                forceRefresh: reconnectOptions.refreshDanmuInfo === true
            });

            if (!this.isRunning || !this.enableGiftSocket) return;

            const host = this._selectDanmuHost(danmuInfo, {
                rotateHost: reconnectOptions.rotateHost === true
            });
            if (!host) {
                throw new Error('未获取到弹幕服务器列表');
            }

            const wsUrl = `wss://${host.host}:${host.wss_port || host.ws_port || 443}/sub`;
            logToTerminal('info', `[LiveStream] 礼物WS目标: ${wsUrl} | hostIndex:${this._giftHostIndex}`);

            const ws = new WebSocketImpl(wsUrl, {
                perMessageDeflate: false
            });

            this._giftWs = ws;

            ws.on('open', () => {
                if (this._giftWs !== ws) return;

                try {
                    this._sendGiftAuth(ws, danmuInfo);
                    this._startGiftHeartbeat(ws);
                    this._startGiftHealthMonitor(ws);
                    this._giftConnectedAt = Date.now();
                    this._giftLastPacketAt = Date.now();
                    this._giftLastBusinessPacketAt = 0;
                    this._giftLastWsDanmuAt = 0;
                    console.log(`礼物 WebSocket 已连接: ${wsUrl}`);
                    logToTerminal('info', `[LiveStream] 礼物WS已连接: ${wsUrl}`);
                } catch (err) {
                    console.error('礼物 WebSocket 初始化失败:', err);
                    logToTerminal('error', `[LiveStream] 礼物WS初始化失败: ${err.message}`);
                }
            });

            ws.on('message', (data) => {
                if (this._giftWs !== ws) return;
                this._handleGiftMessage(data);
            });

            ws.on('close', () => {
                if (this._giftWs === ws) {
                    this._giftWs = null;
                }
                this._clearGiftHeartbeatTimer();
                this._clearGiftHealthTimer();

                if (this.isRunning && this.enableGiftSocket && this._giftShouldReconnect) {
                    this._scheduleGiftReconnect({
                        reason: 'socket-close',
                        rotateHost: true,
                        refreshDanmuInfo: true
                    });
                }
            });

            ws.on('error', (error) => {
                console.error('礼物 WebSocket 错误:', error.message || error);
                logToTerminal('error', `[LiveStream] 礼物WS错误: ${error.message || error}`);
            });
        } catch (error) {
            console.error('礼物 WebSocket 连接失败:', error);
            logToTerminal('error', `[LiveStream] 礼物WS连接失败: ${error.message}`);
            if (this.isRunning && this.enableGiftSocket && this._giftShouldReconnect) {
                this._scheduleGiftReconnect({
                    reason: 'socket-connect-failed',
                    rotateHost: true,
                    refreshDanmuInfo: true
                });
            }
        } finally {
            this._giftSocketInitializing = false;
        }
    }

    _stopGiftSocket() {
        this._giftShouldReconnect = false;
        this._giftSocketInitializing = false;
        this._clearGiftReconnectTimer();
        this._clearGiftHeartbeatTimer();
        this._clearGiftHealthTimer();

        const ws = this._giftWs;
        this._giftWs = null;
        this._giftReconnectOptions = null;

        if (ws) {
            try {
                ws.removeAllListeners();
                ws.close();
            } catch (err) {
                console.warn(`关闭礼物 WebSocket 失败: ${err.message}`);
            }
        }
    }

    _scheduleGiftReconnect(options = {}) {
        this._clearGiftReconnectTimer();
        this._giftReconnectOptions = {
            reason: options.reason || 'scheduled-reconnect',
            rotateHost: options.rotateHost === true,
            refreshDanmuInfo: options.refreshDanmuInfo === true
        };

        logToTerminal(
            'warn',
            `[LiveStream] 计划重连礼物WS | 原因:${this._giftReconnectOptions.reason} | delay:${this.giftReconnectInterval}ms`
        );

        this._giftReconnectTimer = setTimeout(() => {
            this._giftReconnectTimer = null;
            if (this.isRunning && this.enableGiftSocket && this._giftShouldReconnect) {
                this._connectGiftSocket();
            }
        }, this.giftReconnectInterval);
    }

    _clearGiftReconnectTimer() {
        if (this._giftReconnectTimer) {
            clearTimeout(this._giftReconnectTimer);
            this._giftReconnectTimer = null;
        }
    }

    _startGiftHeartbeat(ws) {
        this._clearGiftHeartbeatTimer();

        const heartbeat = () => {
            if (!this._giftWs || this._giftWs !== ws || ws.readyState !== 1) return;

            const roomId = Number(this._resolvedRoomId || this.roomId) || 0;
            const body = {
                roomid: roomId,
                protover: 3,
                platform: 'web',
                type: 2
            };

            try {
                ws.send(this._buildPacket(body, 2, 1));
            } catch (err) {
                console.warn(`礼物 WebSocket 心跳发送失败: ${err.message}`);
            }
        };

        heartbeat();
        this._giftHeartbeatTimer = setInterval(heartbeat, this.giftHeartbeatInterval);
    }

    _clearGiftHeartbeatTimer() {
        if (this._giftHeartbeatTimer) {
            clearInterval(this._giftHeartbeatTimer);
            this._giftHeartbeatTimer = null;
        }
    }

    _startGiftHealthMonitor(ws) {
        this._clearGiftHealthTimer();

        this._giftHealthTimer = setInterval(() => {
            if (!this._giftWs || this._giftWs !== ws || ws.readyState !== 1) return;

            const now = Date.now();
            const packetIdleMs = this._giftLastPacketAt ? now - this._giftLastPacketAt : Infinity;
            const sessionAgeMs = this._giftConnectedAt ? now - this._giftConnectedAt : 0;
            const httpBarrageActive = this._lastBarrageAt > 0 && now - this._lastBarrageAt <= this.giftWsDanmuSilenceTimeout;
            const wsDanmuSilent = !this._giftLastWsDanmuAt || now - this._giftLastWsDanmuAt > this.giftWsDanmuSilenceTimeout;

            if (packetIdleMs >= this.giftNoPacketTimeout) {
                this._restartGiftSocket('no-packet-timeout', {
                    rotateHost: true,
                    refreshDanmuInfo: true
                });
                return;
            }

            if (sessionAgeMs >= this.giftSessionMaxAge) {
                this._restartGiftSocket('session-max-age', {
                    rotateHost: true,
                    refreshDanmuInfo: true
                });
                return;
            }

            if (httpBarrageActive && wsDanmuSilent && sessionAgeMs >= 60000) {
                this._restartGiftSocket('http-ws-danmu-desync', {
                    rotateHost: true,
                    refreshDanmuInfo: false
                });
            }
        }, this.giftHealthCheckInterval);
    }

    _clearGiftHealthTimer() {
        if (this._giftHealthTimer) {
            clearInterval(this._giftHealthTimer);
            this._giftHealthTimer = null;
        }
    }

    _restartGiftSocket(reason, options = {}) {
        if (!this.isRunning || !this.enableGiftSocket) return;

        logToTerminal('warn', `[LiveStream] 主动重启礼物WS | 原因:${reason}`);

        const ws = this._giftWs;
        this._giftWs = null;
        this._clearGiftHeartbeatTimer();
        this._clearGiftHealthTimer();
        this._giftReconnectOptions = {
            reason,
            rotateHost: options.rotateHost === true,
            refreshDanmuInfo: options.refreshDanmuInfo === true
        };

        if (ws) {
            try {
                ws.removeAllListeners();
                if (typeof ws.terminate === 'function') {
                    ws.terminate();
                } else {
                    ws.close();
                }
            } catch (err) {
                console.warn(`关闭礼物 WebSocket 失败: ${err.message}`);
            }
        }

        this._scheduleGiftReconnect(this._giftReconnectOptions);
    }

    _sendGiftAuth(ws, danmuInfo) {
        const roomId = Number(this._resolvedRoomId || this.roomId) || 0;
        const authBody = {
            uid: 0,
            roomid: roomId,
            protover: 3,
            platform: 'web',
            clientver: '1.18.5',
            type: 2,
            key: danmuInfo.token
        };

        ws.send(this._buildPacket(authBody, 7, 1));
    }

    _buildPacket(body, operation, version = 1, sequence = 1) {
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

    _handleGiftMessage(rawData) {
        const buffer = this._normalizeIncomingBuffer(rawData);
        if (!buffer || buffer.length === 0) return;

        this._giftLastPacketAt = Date.now();

        try {
            this._parseGiftPacket(buffer);
        } catch (err) {
            console.error('解析礼物 WebSocket 数据失败:', err);
            logToTerminal('error', `[LiveStream] 礼物WS消息解析失败: ${err.message}`);
        }
    }

    _normalizeIncomingBuffer(rawData) {
        if (!rawData) return null;
        if (Buffer.isBuffer(rawData)) return rawData;
        if (rawData instanceof ArrayBuffer) return Buffer.from(rawData);
        if (Array.isArray(rawData)) {
            return Buffer.concat(rawData.map((chunk) => (Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))));
        }
        if (typeof rawData === 'string') return Buffer.from(rawData, 'utf8');
        return Buffer.from(rawData);
    }

    _parseGiftPacket(buffer) {
        let offset = 0;

        while (offset + 16 <= buffer.length) {
            const packetLength = buffer.readInt32BE(offset);
            if (packetLength < 16 || offset + packetLength > buffer.length) break;

            const headerLength = buffer.readInt16BE(offset + 4);
            const version = buffer.readInt16BE(offset + 6);
            const operation = buffer.readInt32BE(offset + 8);
            const body = buffer.slice(offset + headerLength, offset + packetLength);

            if (operation === 8) {
                logToTerminal('info', `[LiveStream] 礼物WS认证成功 | 房间:${this._resolvedRoomId || this.roomId}`);
            } else if (operation === 5) {
                if (version === 2) {
                    this._parseGiftPacket(zlib.inflateSync(body));
                } else if (version === 3) {
                    this._parseGiftPacket(zlib.brotliDecompressSync(body));
                } else {
                    const payload = this._decodePacketBody(body);
                    if (payload) {
                        this._observeIncomingPayload(payload);
                        this._dispatchGiftPayload(payload);
                    }
                }
            } else if (operation === 3 && body.length >= 4) {
                this._giftLastPacketAt = Date.now();
            }

            offset += packetLength;
        }
    }

    _decodePacketBody(body) {
        if (!body || body.length === 0) return null;

        const text = body.toString('utf8').replace(/\0+$/g, '').trim();
        if (!text) return null;

        try {
            return JSON.parse(text);
        } catch (err) {
            return null;
        }
    }

    _observeIncomingPayload(payload) {
        const rawCommand = payload?.cmd || payload?.command || payload?.type || '';
        const command = String(rawCommand).split(':')[0];
        if (!command) return;

        this._giftLastBusinessPacketAt = Date.now();
        if (command === 'DANMU_MSG') {
            this._giftLastWsDanmuAt = Date.now();
        }
    }

    _dispatchGiftPayload(payload) {
        const rawCommand = payload.cmd || payload.command || payload.type || '';
        const command = String(rawCommand).split(':')[0];
        if (!command || !this._isGiftCommand(command)) return;

        logToTerminal('info', `[LiveStream] 收到礼物事件命令: ${command}`);

        const event = this._normalizeGiftPayload(command, payload);
        if (!event) return;

        logToTerminal(
            'info',
            `[LiveStream] 分发礼物事件: ${event.type} | ${event.nickname} | ${event.giftName}${event.num ? ` x${event.num}` : ''}`
        );

        if (typeof this.onNewGift === 'function') {
            try {
                this.onNewGift(event);
            } catch (err) {
                console.error('礼物回调处理失败:', err);
                logToTerminal('error', `[LiveStream] 礼物回调处理失败: ${err.message}`);
            }
        }
    }

    _isGiftCommand(command) {
        return [
            'SEND_GIFT',
            'COMBO_SEND',
            'GUARD_BUY',
            'SUPER_CHAT_MESSAGE',
            'SUPER_CHAT_MESSAGE_JPN',
            'MESSAGEBOX_USER_GAIN_MEDAL'
        ].includes(command);
    }

    _normalizeGiftPayload(command, payload) {
        const data = payload.data || {};

        if (command === 'MESSAGEBOX_USER_GAIN_MEDAL') {
            return {
                type: command,
                subtype: 'fans_club_join',
                nickname: this._resolveSenderNickname(data, {
                    preferMaskedFields: false,
                    extraCandidates: [data.fan_name]
                }),
                uid: data.uid ?? 0,
                giftName: data.medal_name || '粉丝团',
                num: 1,
                totalCoin: 0,
                coinType: '',
                message: data.toast || data.msg_content || '',
                guardLevel: 0,
                roomId: this._resolvedRoomId || this.roomId,
                raw: payload
            };
        }

        const nickname = this._resolveSenderNickname(data);
        const giftName = this._resolveGiftName(command, data);
        const num = Number(data.num ?? data.combo_num ?? data.gift_num ?? 1) || 1;
        const totalCoin = Number(data.total_coin ?? data.price ?? data.amount ?? data.rmb ?? 0) || 0;
        const coinType = data.coin_type || data.price_type || '';
        const message = data.message || data.msg || data.message_text || '';
        const uid = data.uid ?? data.user_id ?? 0;
        const guardLevel = data.guard_level ?? 0;

        return {
            type: command,
            subtype: command === 'GUARD_BUY' ? 'guard_buy' : 'gift',
            nickname,
            uid,
            giftName,
            num,
            totalCoin,
            coinType,
            message,
            guardLevel,
            roomId: this._resolvedRoomId || this.roomId,
            raw: payload
        };
    }

    _resolveSenderNickname(data, options = {}) {
        const preferMaskedFields = options.preferMaskedFields === true;
        const extraCandidates = Array.isArray(options.extraCandidates) ? options.extraCandidates : [];

        const preferredCandidates = [
            data.sender_uinfo?.base?.name,
            data.sender_uinfo?.base?.origin_info?.name,
            data.uinfo?.base?.name,
            data.user_info?.uname,
            data.user_info?.name,
            ...extraCandidates
        ];

        const fallbackCandidates = [
            data.uname,
            data.username,
            data.user_name,
            data.userName,
            data.fan_name
        ];

        const ordered = preferMaskedFields
            ? [...fallbackCandidates, ...preferredCandidates]
            : [...preferredCandidates, ...fallbackCandidates];

        for (const candidate of ordered) {
            if (typeof candidate === 'string' && candidate.trim()) {
                return candidate.trim();
            }
        }

        return '观众';
    }

    _resolveGiftName(command, data) {
        if (command === 'SUPER_CHAT_MESSAGE' || command === 'SUPER_CHAT_MESSAGE_JPN') {
            return '醒目留言';
        }

        if (command === 'GUARD_BUY') {
            const guardLevel = Number(data.guard_level || 0);
            const guardNameMap = {
                1: '总督',
                2: '提督',
                3: '舰长'
            };
            return guardNameMap[guardLevel] || data.gift_name || data.guard_level_name || '大航海';
        }

        return data.giftName || data.gift_name || data.title || '礼物';
    }

    _selectDanmuHost(danmuInfo, options = {}) {
        const hostList = Array.isArray(danmuInfo?.host_list) ? danmuInfo.host_list : [];
        if (hostList.length === 0) return null;

        if (options.rotateHost === true) {
            this._giftHostIndex = (this._giftHostIndex + 1) % hostList.length;
        } else if (this._giftHostIndex >= hostList.length) {
            this._giftHostIndex = 0;
        }

        return hostList[this._giftHostIndex];
    }

    async _getDanmuInfo(options = {}) {
        if (!options.forceRefresh && this._danmuInfo && this._resolvedRoomId) {
            return this._danmuInfo;
        }

        const realRoomId = await this._resolveRealRoomId();
        const nav = await this._fetchJsonWithHeaders(
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
        this._lastWbiKeys = { imgKey, subKey };

        const query = this._encodeWbiQuery({ id: realRoomId, type: 0 }, imgKey, subKey);
        const url = `https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?${query}`;
        const danmuInfo = await this._fetchJsonWithHeaders(
            url,
            {
                'User-Agent': 'Mozilla/5.0',
                'Referer': `https://live.bilibili.com/${realRoomId}`,
                'Origin': 'https://live.bilibili.com'
            }
        );

        if (danmuInfo?.code !== 0) {
            throw new Error(`getDanmuInfo 失败: ${JSON.stringify(danmuInfo)}`);
        }

        this._danmuInfo = danmuInfo.data;
        if (this._giftHostIndex >= (this._danmuInfo.host_list?.length || 1)) {
            this._giftHostIndex = 0;
        }
        return this._danmuInfo;
    }

    async _resolveRealRoomId() {
        if (this._resolvedRoomId) {
            return this._resolvedRoomId;
        }

        const info = await this._fetchJsonWithHeaders(
            `https://api.live.bilibili.com/room/v1/Room/room_init?id=${this.roomId}`,
            {
                'User-Agent': 'Mozilla/5.0',
                'Referer': `https://live.bilibili.com/${this.roomId}`
            }
        );

        const realRoomId = info?.data?.room_id;
        if (!realRoomId) {
            throw new Error(`无法获取真实房间号: ${JSON.stringify(info)}`);
        }

        this._resolvedRoomId = realRoomId;
        logToTerminal('info', `[LiveStream] 真实房间号: ${this.roomId} -> ${realRoomId}`);
        return realRoomId;
    }

    async _fetchJsonWithHeaders(url, headers) {
        const response = await fetch(url, { headers });
        if (!response.ok) {
            throw new Error(`HTTP ${response.status} ${response.statusText} @ ${url}`);
        }

        return response.json();
    }

    _encodeWbiQuery(params, imgKey, subKey) {
        const mixinKey = this._getMixinKey(imgKey + subKey);
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

    _getMixinKey(origin) {
        return MIXIN_KEY_ENC_TAB.map((index) => origin[index]).join('').slice(0, 32);
    }
}

module.exports = { LiveStreamModule };
