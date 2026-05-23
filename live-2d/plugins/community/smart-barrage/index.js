// plugins/community/smart-barrage/index.js
//
// 工作方式：
//   - 滑动窗口：每 windowSeconds 秒把收集到的弹幕交给 LLM 过滤一次
//   - LLM 从这批弹幕里挑出最多 maxRespond 条值得回复的消息，合并成一条消息让 AI 回应
//   - 带 prefixChar（默认 #）前缀的弹幕跳过过滤，立即单独响应
//   - 监听直播礼物/舰长/粉丝团等事件，单独触发感谢
//
// 模式（mode）：
//   smart  - 所有弹幕走 LLM 过滤
//   prefix - 只响应带 # 前缀的弹幕
//   both   - # 前缀立即响应，其余走 LLM 过滤（默认）

const path = require('path');
const { Plugin } = require('../../../js/core/plugin-base.js');

function loadLiveStreamModule() {
    const modulePath = path.resolve(__dirname, '../../../js/live/LiveStreamModule.js');
    delete require.cache[modulePath];
    const mod = require(modulePath);
    return mod.LiveStreamModule || mod;
}

const SYSTEM_PATCH_ID = 'smart-barrage-context';

class SmartBarragePlugin extends Plugin {
    async onInit() {
        const cfg = this.context.getPluginFileConfig();

        this._roomId = cfg.roomId ?? 30230160;
        this._mode = cfg.mode ?? 'both';
        this._windowMs = (cfg.windowSeconds ?? 15) * 1000;
        this._maxRespond = cfg.maxRespond ?? 3;
        this._maxBuffer = cfg.maxBufferSize ?? 30;
        this._prefixChar = cfg.prefixChar ?? '#';
        this._checkInterval = cfg.checkInterval ?? 5000;

        this._thankGifts = cfg.thankGifts !== false;
        this._giftMinCount = cfg.giftMinCount ?? 1;
        this._giftPromptPrefix = cfg.giftPromptPrefix ?? '[直播感谢]';
        this._thankGuardBuy = cfg.thankGuardBuy !== false;
        this._thankFansClub = cfg.thankFansClub !== false;
        this._thankInteractJoin = cfg.thankInteractJoin !== false;

        this._buffer = [];
        this._windowTimer = null;
        this._liveModule = null;
        this._isSendingThanks = false;
        this._thankQueue = Promise.resolve();
    }

    async onStart() {
        const LiveStreamModule = loadLiveStreamModule();

        this.context.addSystemPromptPatch(
            SYSTEM_PATCH_ID,
            '你现在正在进行 B 站直播。你可能会收到来自观众的直播弹幕，标记为[直播弹幕]。' +
            '请自然地与观众互动，像真实的虚拟主播一样。' +
            '如果收到[点名提问]，优先正面回答。' +
            '如果收到[直播礼物感谢]、[舰长感谢]或[粉丝团入团感谢]，请简短自然地感谢观众。'
        );

        this._liveModule = new LiveStreamModule({
            roomId: this._roomId,
            checkInterval: this._checkInterval,
            onNewMessage: (msg) => this._onBarrage(msg),
            enableGiftSocket: this._thankGifts || this._thankGuardBuy || this._thankFansClub || this._thankInteractJoin,
            onNewGift: (gift) => this._onGift(gift)
        });

        this._liveModule.start();
        this.context.log(
            'info',
            `智能弹幕已启动 | 房间:${this._roomId} | 模式:${this._mode} | 窗口:${this._windowMs / 1000}s | 礼物感谢:${this._thankGifts ? 'on' : 'off'}`
        );
    }

    async onStop() {
        this.context.removeSystemPromptPatch(SYSTEM_PATCH_ID);

        if (this._liveModule) {
            this._liveModule.stop();
            this._liveModule = null;
        }

        if (this._windowTimer) {
            clearTimeout(this._windowTimer);
            this._windowTimer = null;
        }

        this._buffer = [];
        this._isSendingThanks = false;
        this._thankQueue = Promise.resolve();
    }

    _onBarrage({ nickname, text }) {
        if (!text || typeof text !== 'string') return;

        const hasPrefix = text.startsWith(this._prefixChar);

        if (hasPrefix && (this._mode === 'prefix' || this._mode === 'both')) {
            const clean = text.slice(this._prefixChar.length).trim();
            if (!clean) return;

            this.context.log('info', `[点名] ${nickname}: ${clean}`);
            this.context.sendMessage(
                `[直播弹幕-点名提问] ${nickname} 向你提问：${clean}`
            ).catch((e) => this.context.log('error', `sendMessage 失败: ${e.message}`));
            return;
        }

        if (this._mode === 'prefix') return;

        this._buffer.push({ nickname, text });
        if (this._buffer.length > this._maxBuffer) {
            this._buffer.shift();
        }

        if (!this._windowTimer) {
            this._windowTimer = setTimeout(() => this._flushWindow(), this._windowMs);
        }
    }

    _onGift(event) {
        if (!event) return;

        if (event.type === 'GUARD_BUY' && !this._thankGuardBuy) return;
        if (event.subtype === 'fans_club_join' && !this._thankFansClub) return;
        if (event.subtype === 'fans_club_join' && !this._thankInteractJoin) return;
        if (event.type !== 'GUARD_BUY' && event.type !== 'MESSAGEBOX_USER_GAIN_MEDAL' && !this._thankGifts) return;

        const count = Number(event.num || 0) || 0;
        if (event.subtype === 'gift' && count < this._giftMinCount) return;

        const prompt = this._buildThankPrompt(event);
        if (!prompt) return;

        this.context.log(
            'info',
            `[感谢事件] ${event.nickname} -> ${event.giftName}${event.num ? ` x${event.num}` : ''}`
        );

        this._thankQueue = this._thankQueue
            .then(async () => {
                this._isSendingThanks = true;
                await this.context.sendMessage(prompt, {
                    allowInterrupt: false,
                    waitForIdle: true
                });
            })
            .catch((e) => {
                this.context.log('error', `感谢发送失败: ${e.message}`);
            })
            .finally(() => {
                this._isSendingThanks = false;
            });
    }

    async _flushWindow() {
        this._windowTimer = null;
        if (this._buffer.length === 0) return;

        if (this._isSendingThanks) {
            this.context.log('info', '感谢正在进行，普通弹幕批次暂缓处理');
            this._windowTimer = setTimeout(() => this._flushWindow(), 1500);
            return;
        }

        const batch = this._buffer.slice();
        this._buffer = [];

        this.context.log('info', `开始过滤弹幕批次，共 ${batch.length} 条`);

        try {
            const selected = await this._filterWithLLM(batch);
            if (selected.length === 0) {
                this.context.log('info', '本批弹幕无值得回复的内容，跳过');
                return;
            }

            const prompt = this._buildPrompt(selected);
            this.context.log('info', `选中 ${selected.length} 条弹幕，发起回复`);
            await this.context.sendMessage(prompt);
        } catch (e) {
            this.context.log('error', `弹幕批次处理失败: ${e.message}`);
        }
    }

    async _filterWithLLM(batch) {
        const numbered = batch.map((m, i) => `${i + 1}. ${m.nickname}：${m.text}`).join('\n');

        const prompt =
            `你是一个直播间 AI 主播的助理，负责筛选值得主播回复的弹幕。\n` +
            `以下是直播间最近 ${this._windowMs / 1000} 秒内的弹幕，请从中挑选最多 ${this._maxRespond} 条最值得回复的。\n` +
            `优先选：有实际内容的问题、有趣的评论、值得互动的话题。\n` +
            `忽略：刷屏、无意义的“哈哈哈”、纯表情、重复问题。\n` +
            `如果整批都没有值得回复的，返回空数组。\n` +
            `只返回选中的序号，JSON 数组格式，例如 [1,3] 或 []，不要有其他文字。\n\n` +
            `弹幕列表：\n${numbered}`;

        try {
            const raw = await this.context.callLLM(prompt, { temperature: 0.2 });
            const match = raw.match(/\[[\d,\s]*\]/);
            if (!match) return [];

            const indices = JSON.parse(match[0]);
            return indices
                .filter((i) => Number.isInteger(i) && i >= 1 && i <= batch.length)
                .slice(0, this._maxRespond)
                .map((i) => batch[i - 1]);
        } catch (e) {
            this.context.log('warn', `LLM 过滤调用失败: ${e.message}`);
            return [];
        }
    }

    _buildPrompt(selected) {
        if (selected.length === 1) {
            return `[直播弹幕] ${selected[0].nickname} 说：${selected[0].text}`;
        }

        const lines = selected.map((m) => `- ${m.nickname}：${m.text}`).join('\n');
        return `[直播弹幕] 观众们说：\n${lines}`;
    }

    _buildThankPrompt(event) {
        const basePrefix = this._giftPromptPrefix;

        if (event.type === 'GUARD_BUY') {
            const guardTitle = event.giftName || '舰长';
            return `${basePrefix} ${event.nickname} 刚刚开通了 ${guardTitle}。请你自然、简短地感谢这位观众，语气像直播中的虚拟主播。`;
        }

        if (event.subtype === 'fans_club_join') {
            return `${basePrefix} ${event.nickname} 刚刚加入了粉丝团。请你自然、简短地欢迎并感谢这位观众，语气像直播中的虚拟主播。`;
        }

        const countText = event.num > 1 ? ` x${event.num}` : '';
        const valueText = event.totalCoin > 0
            ? `，价值约 ${this._formatGiftValue(event.totalCoin, event.coinType)}`
            : '';
        const messageText = event.message ? `，并附言：${event.message}` : '';

        return `${basePrefix} ${event.nickname} 刚刚送出了 ${event.giftName}${countText}${valueText}${messageText}。请你自然、简短地感谢这位观众，语气像直播中的虚拟主播。`;
    }

    _formatGiftValue(totalCoin, coinType) {
        if (coinType === 'gold') {
            const cny = totalCoin / 1000;
            if (Number.isFinite(cny) && cny > 0) {
                return `${cny} 元`;
            }
        }

        return `${totalCoin} ${coinType || 'coin'}`;
    }
}

module.exports = SmartBarragePlugin;
