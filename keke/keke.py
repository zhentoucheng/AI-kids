# -*- coding: utf-8 -*-
"""
家庭小克 v0.1.0  ——  基于摄像头 + 语音的家庭 AI 助理（被动应答型）

跟小考官（kaoguan.py）是兄弟，分工明确：
  小考官 = 主动出题的**考官**（客厅有人就按计划考你，不等你开口）
  小克   = 被动应答的**助理**（你问它才答，主打"看画面"）
  两个角色别揉进一个进程 —— 人格会拧巴，而且抢同一个音箱。

─────────────────────────────────────────────────────────────
耳朵：为什么这么绕
─────────────────────────────────────────────────────────────
小爱音箱（L05C）**不给第三方音频流**，也拿不到麦克风。但小爱在小米云里
已经把你的话转成文字了 —— 去轮询对话记录接口就能读到：

  GET https://userprofile.mina.mi.com/device_profile/v2/conversation
  返回 records[].query              = 小爱听到的原话（就是你"说"的内容）
       records[].answers[].tts.text = 小爱自己怎么答的（纯文本，可解析）

实测（2026-10-04）：
  · 延迟 lag 0.3~1.3 秒（2 秒轮询一次），完全够实时对话
  · "小爱同学"唤醒前缀**不一定**被转写（4 条记录全都没有）→ 不能当判据
  · ★ 唤醒词会被转成**同音字**：喊"小克"，云端一次转出"小客"、一次"小柯"
      → 精确字符串匹配必挂，必须按同音字表匹配（见 cfg.wake.chars）

─────────────────────────────────────────────────────────────
两条触发通道
─────────────────────────────────────────────────────────────
  A 显式唤醒    query 命中"小克"及同音变体 → 必答
  B 自动补位    小爱**明确认输** 且 query 含视觉意图 → 补位答

B 通道是这件事情最妙的地方。原本担心"小爱先瞎答一句、小克再答一遍"很尴尬，
实测推翻了这个判断 —— 小爱答不上来时的措辞是**固定话术**：

    「这可把我难住了，看来要更努力学习了」
    「这个我暂时还回答不上诶，我要再学习学习」
    「该指令暂不支持展示」

而视觉问答恰好是小爱做不到的（它没摄像头）。于是流程变成：
    你说 → 小爱认输「这可把我难住了」→ 小克接着说出正确答案
逻辑天然顺，还**不用喊"小克"**，比纯语音助手更自然。

⚠️ 但必须卡双重条件。小爱对一大堆无关话题也会说"这可把我难住了"，
   只按"它认输就接管"会让小克在客厅到处插嘴（孩子说句"这题我不会"都可能触发）。
   所以 B 通道要求 认输 + 视觉意图词 同时成立。

─────────────────────────────────────────────────────────────
嘴：复用小考官那条播报通道
─────────────────────────────────────────────────────────────
HA  REST  text.set_value → text.xiaomi_l05c_XXXX_play_text
（小爱接 HA 后只能"念文本"，播不了 HA 合成的音频；这条通道小考官已经跑通）

─────────────────────────────────────────────────────────────
跟小考官抢同一台音箱怎么办
─────────────────────────────────────────────────────────────
  · 订阅 kaoguan/state（retained）读 asking 字段 → 考官在出题时小克不说话
  · 小克自己也发 keke/state，对称设计，方便运维
  · 播报前按文本长度估算小爱要说多久，等它说完再开口，别打断

用法
  python keke.py                          # 常驻
  python keke.py --test-pull              # 只拉一次对话记录并打印（验耳朵）
  python keke.py --test-ask "客厅有几个人"  # 走完整链路（抓帧+模型），但不播报
  python keke.py --test-speak "你好"       # 只测播报通道
  python keke.py --judge "小客看看客厅"      # 只做判定，不联网不播报（调规则用）
  python keke.py --state                  # 打印生效配置（不含密钥）
"""
import argparse
import base64
import hashlib
import json
import logging
import os
import random
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
import http.cookiejar
from datetime import datetime, timezone

import yaml

try:
    import paho.mqtt.client as mqtt
except ImportError:                     # 没装 paho 也能跑，只是没有远程通道
    mqtt = None

BASE = os.path.dirname(os.path.abspath(__file__))
# v0.2.0 (2026-11-04)：进门链路重做。当天 5 次真实开门只认对 1 次，逐条对账后
#   改了五处：① 窗口语义搞错（_in_window 是"从 at 往后"不是对称），姐姐 15:26
#   到家时窗口还没开；② 上学日分支漏传 door_only，退到摄像头人影兜底；
#   ③ 信号源顺序把最不可靠的 lock_action 放第一位；④ 门锁心跳 unavailable→
#   outside_unlock 被当成开门；⑤ 不核对开门的是谁，窗口放宽后会喊错人。
VERSION = "v0.2.0"
MQTT_STATE_TOPIC = "keke/state"
MQTT_CMD_TOPIC = "keke/cmd"

log = logging.getLogger("keke")


# ----------------------------------------------------------------------------
# 默认配置
# ----------------------------------------------------------------------------
DEFAULT_CFG = {
    "ha": {
        "url": "http://192.168.1.100:8123",
        "token": "",
        "tts_entity": "text.xiaomi_l05c_XXXX_play_text",
    },
    # 小米云凭据（从飞牛 190 的 xiaomusic 同步过来，只读挂载）
    "mi": {
        "token_path": "/mi/.mi.token",
        # ★★ device_id 必须是**音箱**的，不能拿 token 里那个！
        #   2026-10-04 踩坑实锤：token 的 deviceId 字段（16 字符，如 "ubfQBXnW…"）
        #   是**登录设备的 ID**；接口按 deviceId 过滤记录，传错 → code=0 但
        #   records 恒空，看着像"接口没数据"，其实查错了设备。
        #   正确值来自 xiaomusic 的 setting.json：devices[did].device_id（UUID）
        #   实测：传音箱 UUID → 立刻拿到历史；传 token 里的 → 0 条。
        #   留空则自动从下面的 device_file 读（sync-mitoken.py 会生成）。
        "device_id": "",
        "device_file": "/mi/mi-device.json",
        "hardware": "L05C",
        "timeout": 15,
    },
    "mqtt": {"host": "192.168.1.100", "port": 11883, "username": "", "password": "",
             "client_id": "keke"},
    # 摄像头取帧：go2rtc 无需鉴权，width 参数由它自己缩放
    "camera": {"snapshot_url": "http://192.168.1.100:1984/api/frame.jpeg?src=c300&width=960",
               "timeout": 15},
    "llm": {
        "enabled": True,
        "base_url": "http://192.168.1.1:31417/v1",
        "api_key": "",
        # 支持视觉、国内直连可用的（同小考官实测结论）
        "models": ["agnes-2.5-flash", "agnes-2.0-flash",
                   "dots-studio/dots-3-note-preview:free", "glm-4.6v-flash"],
        "timeout": 60,
        "max_tokens": 400,
        "temperature": 0.7,
    },
    # ── 唤醒判定 ─────────────────────────────────────────────────────────
    "wake": {
        # ★ 同音字表：小米云 STT 只做同音转写、不认专名。
        #   实测喊"小克"转出过"小客""小柯"两种 → 必须按字表匹配。
        #   宁可多列几个同音字（误触发有 B 通道的双重条件兜底，
        #   漏触发则是"喊了没反应"，体感差得多）。
        #   2026-10-04 二次实测：又抓到"小哥"（"小哥你看一下客厅有什么"）→ 补"哥"
        "chars": "克客柯科可刻课嗑恪溘哥",
        # ★ 假阳性词表：必须是**含"小"+同音字的整词**，判定规则是
        #   "该词覆盖了唤醒匹配位"才否决（见 is_wake）。
        #   ⚠️ 早期版本写的是"课堂/功课/客厅"这种不含"小"的词 —— 那是**死配置**，
        #   因为这类词前面没有"小"，永远不会被 wake_re 匹配上，写了也没用。
        "deny": ["小哥哥", "小哥们", "小哥俩", "小哥儿",
                 "小可爱", "小可怜", "小可人",
                 "小课堂", "小课本", "小课程", "小课题", "小考试",
                 "小科学", "小科长", "小柯基", "小刻苦", "小磕碰"],
    },
    # ── 小爱"认输"话术（B 通道第一条件）────────────────────────────────
    #   ⚠️ 这是**小爱的回复**里出现这些词，不是用户说的。
    #   小爱话术会变，所以放配置里好调；命中任一即算"它答不上来"。
    "surrender_phrases": [
        "这可把我难住了",
        "我暂时还回答不上",
        "回答不上",
        "再学习学习",
        "我还需要学习",
        "我还在学习中",          # ← 2026-10-04 漏判元凶："对不起，我还在学习中!"
        "先聊点别的",            # ← "这个问题我还在学习中，先聊点别的吧"
        "该指令暂不支持",
        "暂不支持",
        "没有找到可控制的设备",
        "听不懂",
        "没听懂",
        "我还不会",
        "我还没学会",
        "超出我的能力",
    ],
    # ★ 兜底正则：小爱话术是**随机变体**（一天内抓到 4 种不同说法），
    #   逐条罗列 phrase 总有漏的。这条正则是"看着像答不上来"的宽口径，
    #   命中即算认输；再由 vision_intent 第二道关卡防误触发。
    "surrender_regex": r"学习中|学一学|学习学习|没学会|还不会|回答不上|难住|"
                       r"暂不支持|没找到可控制|听不懂|没听懂|超出.{0,4}能力|"
                       r"帮不上|不太会|不明白|不会这个",
    # ── 视觉意图词（B 通道第二条件）────────────────────────────────────
    "vision_intent": [
        "看", "看看", "看见", "看到", "瞧", "几个人", "几人", "有没有人",
        "是什么", "什么样的", "什么颜色", "谁", "在哪", "在做什么", "在干嘛",
        "手里", "身上", "客厅", "房间", "画面", "摄像头", "镜头",
        "认得", "认识", "穿", "拿",
    ],
    "trigger": {
        "auto_fill": True,           # B 通道总开关
        "poll_interval": 2.0,        # 轮询间隔（秒）。实测 2s → lag 0.3~1.3s
        "speak_delay": 2.5,          # 读到记录后等这么久再开口，让小爱把话说完
        "min_gap_seconds": 8,        # 两次回答最短间隔，防连击
        "max_len": 120,              # 用户单句超过这个长度不处理（多半是别的语音）
        "tts_chars_per_second": 4.5, # 中文 TTS 语速，估"说完要多久"
    },
    "yield_to_kaoguan": {
        "enabled": True,
        "asking_quiet_seconds": 45,  # 考官在出题/报答案时，安静这么久
    },
    "state_file": os.path.join(BASE, "keke-state.json"),
}


def deep_merge(a, b):
    out = dict(a or {})
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_cfg(path):
    if not path or not os.path.exists(path):
        log.warning("配置文件不存在：%s → 用默认值（多半跑不起来，密钥都在配置里）", path)
        return dict(DEFAULT_CFG)
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return deep_merge(DEFAULT_CFG, raw)


# ----------------------------------------------------------------------------
# HA 客户端（照抄小考官，只留播报需要的）
# ----------------------------------------------------------------------------
class HAClient:
    def __init__(self, cfg, dry=False):
        self.url = cfg["url"].rstrip("/")
        self.token = cfg["token"]
        self.tts = cfg["tts_entity"]
        self.dry = dry
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        self.last_speak_at = 0.0
        self.last_speak_text = ""

    def _req(self, path, method="GET", data=None, timeout=12):
        h = {"Authorization": "Bearer " + self.token} if self.token else {}
        body = None
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            h["Content-Type"] = "application/json"
        req = urllib.request.Request(self.url + path, data=body, headers=h, method=method)
        return urllib.request.urlopen(req, timeout=timeout, context=self.ctx).read()

    def speak(self, text):
        log.info("🔊 小克播报：%s", text)
        if self.dry:
            log.info("[DRY] 不真播")
            return True
        try:
            self._req("/api/services/text/set_value", "POST",
                      {"entity_id": self.tts, "value": text})
            self.last_speak_at = time.time()
            self.last_speak_text = text
            return True
        except urllib.error.HTTPError as e:
            log.error("播报失败 HTTP %s：%s", e.code,
                      e.read().decode("utf-8", "replace")[:200])
            return False
        except Exception as e:
            log.error("播报异常：%s", e)
            return False


# ----------------------------------------------------------------------------
# 小米云对话记录（耳朵）
# ----------------------------------------------------------------------------
class MiClient:
    """轮询小米云对话记录接口。

    凭据来自 xiaomusic 容器的 .mi.token（JSON）：
      serviceToken = micoapi[1]   ← ★ 注意索引 1，micoapi[0]（24 字符）不是 token
      userId       = userId
      deviceId     = 接口只校验"存在"，用 token 里的即可
    cookie 模板（权威来源 xiaomusic const.py）：
      "deviceId={d}; serviceToken={s}; userId={u}"
    """

    RELOAD_THROTTLE = 300   # 401 后重载凭据的最短间隔（秒），免得每轮 poll 都刷日志

    def __init__(self, cfg, ctx=None):
        self.cfg = cfg
        self.ctx = ctx or ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE
        self.cookie = None
        self._loaded = False
        # ★ 凭据自愈（2026-10-06 事故后加）：见 _watch_credential / pull 的 401 分支
        self._tok_sig = None        # 磁盘上凭据文件的指纹，变了就热重载
        self._last_reload_try = 0   # 401 重载的节流时间戳，别每轮都试

    def _ensure(self):
        """★ v0.1.5：凭据改成**懒加载**。

        原来是 __init__ 里就 `_load_token()`，没挂 /mi 目录直接 SystemExit ——
        可本地想跑 `--steward-dry` / `--test-look` / `--judge` 这些**不碰耳朵**的
        调试，也被一起打死。凭据等真要用（pull）的时候再要就行。
        """
        if not self._loaded:
            self._load_token()
            self._loaded = True

    def _load_token(self):
        p = self.cfg["token_path"]
        if not os.path.exists(p):
            raise SystemExit("找不到小米 token：%s（把 xiaomusic 的 conf 目录只读挂进来）" % p)
        with open(p, encoding="utf-8") as f:
            tok = json.load(f)
        mico = tok.get("micoapi") or []
        if not isinstance(mico, list) or len(mico) < 2:
            raise SystemExit("micoapi 结构不对（应为 [deviceId, serviceToken]）：%r"
                             % type(mico))
        service_token = mico[1]
        user_id = str(tok.get("userId") or "")
        if not service_token or not user_id:
            raise SystemExit("token 里缺 serviceToken / userId")

        # ★★ device_id 三层解析（详见 DEFAULT_CFG["mi"] 的注释）
        device_id = (self.cfg.get("device_id") or "").strip()
        hardware = self.cfg.get("hardware") or "L05C"
        origin = "config"
        if not device_id:
            df = self.cfg.get("device_file")
            if df and os.path.exists(df):
                try:
                    with open(df, encoding="utf-8") as f:
                        d = json.load(f)
                    device_id = (d.get("device_id") or "").strip()
                    hardware = d.get("hardware") or hardware
                    origin = "device_file（%s）" % (d.get("name") or "?")
                except Exception as e:
                    log.warning("读设备文件 %s 失败：%s", df, e)
        if not device_id:
            device_id = (tok.get("deviceId") or "").strip()
            origin = "token.deviceId ⚠️（这是登录设备 ID，多半查不到记录）"
            log.warning("没配 mi.device_id / 读不到 mi-device.json，回落到 token.deviceId。"
                        "如果一直拉不到对话记录，就是这个原因 —— 跑一次 sync-mitoken.py")
        self.device_id = device_id
        self.hardware = hardware
        self.cookie = "deviceId=%s; serviceToken=%s; userId=%s" % (
            device_id, service_token, user_id)
        log.info("小米凭据已加载（userId=%s，token %d 字符，设备 %s… 来源 %s）",
                 user_id, len(service_token), device_id[:8], origin)
        self._tok_sig = self._file_sig()

    # -- 凭据自愈（2026-10-06 事故教训）----------------------------------
    # 事故回放：飞牛 190 的 xiaomusic 刷新了凭据，群晖这边还是 10-04 的旧值，
    # pull 一路 401，容器里缓存的就是过期 cookie，手工同步完还得再手工重启。
    # 现在两道保险：
    #   ① 每次 pull 前比对磁盘指纹，外部同步脚本一写就热重载，不用重启
    #   ② pull 撞上 401/403 自己重载重试一次（节流，避免每轮刷日志）
    def _file_sig(self):
        """凭据文件的 md5+长度。文件不存在/读不了就返回 None（不误判为变化）。"""
        try:
            with open(self.cfg["token_path"], "rb") as f:
                b = f.read()
            return (hashlib.md5(b).hexdigest(), len(b))
        except Exception:
            return None

    def _reload(self, why):
        try:
            self._load_token()
            log.warning("♻️ 凭据已重载（原因：%s）", why)
            return True
        except Exception as e:
            log.error("凭据重载失败（%s）：%s", why, e)
            return False

    def _watch_credential(self):
        sig = self._file_sig()
        if sig and self._tok_sig and sig != self._tok_sig:
            self._reload("凭据文件已变化（多半是同步脚本写了新 token）")

    def pull(self, limit=6):
        """拉最近 N 条对话记录。失败抛异常，由调用方决定是否吞掉。

        ★ 401/403 自愈（2026-10-06 事故后加）：凭据是从飞牛同步来的，
          xiaomusic 一刷新就过期。撞上认证错误先重载再试一次，还不行才抛。
        """
        self._ensure()
        self._watch_credential()
        for _attempt in range(2):
            try:
                return self._pull_once(limit)
            except urllib.error.HTTPError as e:
                if e.code not in (401, 403) or _attempt:
                    raise
                if time.time() - self._last_reload_try < self.RELOAD_THROTTLE:
                    raise
                self._last_reload_try = time.time()
                if not self._reload("HTTP %s（小米认证失败）" % e.code):
                    raise

    def _pull_once(self, limit=6):
        ts = int(time.time() * 1000)
        url = ("https://userprofile.mina.mi.com/device_profile/v2/conversation"
               "?source=dialogu&limit=%d&timestamp=%d&hardware=%s"
               % (limit, ts, getattr(self, "hardware", None)
                  or self.cfg.get("hardware", "L05C")))
        req = urllib.request.Request(url, headers={
            "Cookie": self.cookie,
            "User-Agent": "MiHome/6.0",
        })
        raw = urllib.request.urlopen(req, timeout=self.cfg.get("timeout", 15),
                                     context=self.ctx).read()
        d = json.loads(raw.decode("utf-8", "replace"))
        if d.get("code") != 0:
            raise RuntimeError("小米云返回 code=%s msg=%s" % (d.get("code"), d.get("message")))
        data = d.get("data")
        if isinstance(data, str):          # 实测 data 是**二次编码的 JSON 字符串**
            data = json.loads(data)
        return (data or {}).get("records") or []


# ----------------------------------------------------------------------------
# 主逻辑
# ----------------------------------------------------------------------------
class Keke:
    PRAISE_NEUTRAL = ["", "我看看啊。", "嗯，让我看一眼。"]

    def __init__(self, cfg, dry=False):
        self.cfg = cfg
        self.dry = dry
        self.ha = HAClient(cfg["ha"], dry=dry)
        self.mi = MiClient(cfg["mi"], ctx=self.ha.ctx)
        self.ctx = self.ha.ctx

        self.wake_re = self._build_wake_re()
        self.deny = [d for d in (cfg["wake"].get("deny") or []) if d]
        self.surrender = [s for s in (cfg.get("surrender_phrases") or []) if s]
        _sr = cfg.get("surrender_regex") or ""
        self.surrender_re = re.compile(_sr) if _sr else None
        self.intent = [w for w in (cfg.get("vision_intent") or []) if w]

        self.seen = set()            # requestId 去重
        self.seen_order = []
        self.last_time = None        # 已处理到的时间戳（毫秒）
        self.last_answer_at = 0.0
        self.answering = False
        self.echoes = []             # 最近播报的文本，防回环
        self.kaoguan_asking_at = 0.0 # 考官最近一次"在说话"的时刻
        self.kaoguan_state = {}
        # ★ 2026-10-19：Frigate 人脸识别结果缓存 {名字: (识别时刻, 置信度)}。
        #   以前只用系统提示词里那句「家里有两个孩子」，画面里是谁全靠模型猜；
        #   而 Frigate 侧 face_recognition 一直在跑（近 6h 认到妹妹 25 次、
        #   姐姐 6 次，分数 0.87~1.0），只是没人订阅 frigate/events 用起来。
        self.faces = {}
        self._fr_op = None           # Frigate API 的 cookie opener（登录一次复用）
        self._fr_jar = None
        self._fr_login_at = 0.0
        self._fr_last_try = 0.0
        self.mq = None
        self.started_at = time.time()
        self._last_state_pub = 0.0
        self.today = datetime.now().strftime("%Y-%m-%d")
        self.today_count = 0
        self.today_fill = 0
        # 上一次长内容任务（作文/故事）的尾巴，用于「继续写」接上下文
        self.last_task = {}
        # 管家模式（主动说话），在 run() 里实例化
        self.steward = None
        # 多久内说「继续」算续写（秒）。配置项 continue_window_s，默认 10 分钟
        self.cont_window = float((cfg.get("trigger") or {}).get("continue_window_s") or 600)
        self.load_state()

    # -- 唤醒词 ------------------------------------------------------------
    def _build_wake_re(self):
        chars = self.cfg["wake"].get("chars") or "克"
        cls = re.escape(chars)
        # 「小克」「小 克」「小克克」都算；中间允许空格（STT 偶尔插空格）
        return re.compile(r"小\s*[%s]" % cls)

    def is_wake(self, q):
        """句中任意一个"小X"匹配位**没有被 deny 词覆盖**，就算唤醒。

        覆盖 = deny 词的起止区间含住匹配位起点。这样：
          「小哥哥在干嘛」→ 匹配位被"小哥哥"盖住 → 否决
          「小克，外卖小哥说啥」→ 匹配位没被盖住 → 唤醒 ✅
        （旧实现只比 q[m.start():m.start()+len(d)]，deny 词不含"小"就永远不生效。）
        """
        q = q or ""
        for m in self.wake_re.finditer(q):
            covered = False
            for d in self.deny:
                if not d:
                    continue
                p = q.find(d)
                while p != -1:
                    if p <= m.start() < p + len(d):
                        covered = True
                        break
                    p = q.find(d, p + 1)
                if covered:
                    break
            if not covered:
                return True
        return False

    # -- 判据 --------------------------------------------------------------
    @staticmethod
    def answer_text(rec):
        """把小爱的回复拼成纯文本。answers[].tts.text 就是它说的话。"""
        parts = []
        for a in (rec.get("answers") or []):
            t = ((a or {}).get("tts") or {}).get("text")
            if t:
                parts.append(t)
        return " ".join(parts)

    def is_surrender(self, ans_text):
        """小爱答不上来？先查精确话术，再走兜底正则（话术是随机变体，全靠列举必漏）。"""
        if not ans_text:
            return False
        if any(p in ans_text for p in self.surrender):
            return True
        return bool(self.surrender_re and self.surrender_re.search(ans_text))

    def has_vision_intent(self, q):
        return any(w in (q or "") for w in self.intent)

    def is_echo(self, q):
        """小克自己播的话如果被当成输入，会造成自问自答死循环。"""
        q = (q or "").strip()
        if not q:
            return True
        for e in self.echoes[-5:]:
            if not e:
                continue
            if q == e or (len(q) > 6 and q in e):
                return True
        return False

    def judge(self, q, ans_text=""):
        """返回 (是否响应, 模式, 理由)。--judge 用它调规则。"""
        if self.is_echo(q):
            return False, None, "回环（跟最近播报重复）"
        wake = self.is_wake(q)
        sur = self.is_surrender(ans_text)
        vis = self.has_vision_intent(q)

        # 假阳性词一票否决（两条通道都拦，无条件）：
        # "小哥哥在干嘛" 这种虽然形状像提问，但明显在说人，不是问摄像头。
        # 注意是**无条件**的 —— is_wake 为假时也拦，否则会从 fill 通道漏进去。
        if any(d and d in q for d in self.deny):
            return False, None, "命中假阳性词（小哥哥/小可爱…）"
        # 补位通道额外限长：长句多半是人在跟别人聊天/电视声，
        # 真实抓到的案例——"小爱同学他们都可以基于这个摄像头然后跟…"（29字）
        # 小爱答不上、又含"摄像头"，差点被当成视觉提问。
        if not wake and len(q or "") > int(self.cfg["trigger"].get("fill_max_len") or 20):
            return False, None, "无唤醒词且句子过长（%d字）" % len(q or "")
        if wake:
            return True, "wake", "命中唤醒词"
        # ★ v0.1.10：补位通道对「创作类请求」豁免视觉意图词。
        #   起因是 20:41:01 的实锤 —— 兴哥说
        #   「小爱同学，桌上有个蓝色水杯，帮我写一篇100字作文」，
        #   这句话**没有"小克"**，走补位通道。补位通道要求
        #   「小爱认输 + 视觉意图词」，而"帮我写一篇作文"里没有
        #   摄像头/画面/看/认得这类词 → 被"无视觉意图词"直接忽略，
        #   结果作文没写。
        #
        #   但这个门槛本来就是为「小爱答不上画面问题」设计的（比如"水杯什么颜色"），
        #   对作文/故事/总结这类纯创作请求根本不适用 —— 人家根本不想让你看画面。
        #   只要 query 命中 LONG_RE，就当"视觉意图"成立，让补位通道接住。
        #   ★ 不要连 sur 一起豁免：小爱没认输说明它自己答了，那就不该抢。
        if self.cfg["trigger"].get("auto_fill") and sur and (
                vis or bool(self.LONG_RE.search(q or ""))):
            return True, "fill", ("小爱认输 + 视觉意图"
                                  if vis else "小爱认输 + 创作请求")
        why = []
        if not self.cfg["trigger"].get("auto_fill"):
            why.append("补位通道已关")
        else:
            if not sur:
                why.append("小爱没认输")
            elif not vis and not self.LONG_RE.search(q or ""):
                why.append("无视觉意图词（也不是创作请求）")
        return False, None, "；".join(why) or "不满足条件"

    # -- 视觉问答 ----------------------------------------------------------
    def grab_frame(self):
        url = (self.cfg.get("camera") or {}).get("snapshot_url")
        if not url:
            return None
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "keke/%s" % VERSION})
            return urllib.request.urlopen(
                req, timeout=(self.cfg["camera"].get("timeout") or 15),
                context=self.ctx).read()
        except Exception as e:
            log.warning("取帧失败（%s）：%s", url, e)
            return None

    def grab_frames(self, n=3, gap=1.5):
        """★ v0.1.8 抓 n 帧（间隔 gap 秒）返回列表，**至少要 1 帧才算成功**。

        ★★ 为什么改成多帧（2026-10-04 19:55 实测，这是本轮最硬的证据）：
          同一静止画面、同一模型 agnes-2.5-flash，连问 15 次：
            人数 1 人 13/15、3 人 1/15、解析失败 1/15   → 人数基本稳
            状态 打闹 8/15、安静 2/15、空 4/15        → **状态是纯随机噪声**
          同一秒的三次判定可以是「打闹 / 打闹 / 安静」。
          也就是说打闹检测在这种单帧模式下**不可能可靠**，
          调 min_people、trust_state 都是治标——判据本身没有信息量。
          而"多传几张图给模型自己综合判断"，一次请求就够，
          实测耗时和单帧一样（平均 5.0s），比"同一请求问三次再投票"省 2/3 时间。
        """
        out = []
        for i in range(max(1, int(n or 1))):
            if i:
                time.sleep(gap)
            f = self.grab_frame()
            if f:
                out.append(f)
        return out or None

    # ★★ v0.1.4 重写：旧版整段都在说"你是来看画面的"，还明写"唯一的能力是看摄像头画面"。
    #    结果 13:40 兴哥说「此情此景你帮我写一篇作文」，模型老老实实地**报了一遍画面内容**
    #    —— 它的 prompt 只允许它干这件事。long_form 当时只改了第 1 条规则，
    #    后面六条（"只说你真正看到的东西""直接描述内容"）仍把模型往"描述"上按。
    #    改法：先把「用户到底要什么」摆在最前面分三类，画面重新定位成**素材**而不是任务。
    def _system_prompt(self, long_form=False):
        head = (
            "你是家庭助理「小克」，接在一台语音音箱上。你能拿到一张摄像头实时画面当参考，"
            "但**用户要你做的事不一定是「描述画面」**。先判断用户想干什么，再决定怎么答：\n"
            "· 问画面（几个人、在做什么、什么颜色、谁在哪、手里拿的什么）"
            "→ 直接说出你看到的东西，不要带「画面里」这种前缀；\n"
            "· 让你创作或出主意（写作文/日记/故事、写一段话、起名字、提建议、点评、吐槽）"
            "→ 把画面当**场景素材**，去完成用户要的那件事，不要只交一份画面清单；\n"
            "· 闲聊或别的事 → 像正常人一样回话，跟画面无关就别硬扯画面。\n")
        common = (
            "通用要求：\n"
            "- 只讲有依据的内容。看不清就说看不清，绝不编造画面里没有的东西。\n"
            "- 不要提「照片」「图片」「画面里」这些词，直接说内容。\n"
            "- 不复述问题，不寒暄，不解释自己在做什么，直接给结果。\n"
            "- 严禁复述或总结上面这段说明（不要说「用户让我」「我扮演」「我的任务是」"
            "「用户问」之类）。\n"
            "- 严禁输出分析步骤、提纲、编号列表或思考过程，只输出最终内容本身。\n"
            "- 家里有两个孩子：姐姐（六年级，**戴眼镜**，身高约 153cm）、"
            "妹妹（二年级，**不戴眼镜**，身高约 127cm）。"
            "需要时可以用这个背景，但别主动提。\n")
        if long_form:
            return (head + common +
                    "\n这次是**创作类请求**（作文/故事/日记/文案），要求：\n"
                    "1. 交一篇**完整的作品**，不是画面清单。禁止写成「有两个人在客厅、"
                    "桌上有台电脑」这种罗列式描述。\n"
                    "2. 以画面里的真实场景为底子，可以合理想象细节（天气、声音、光线、心情），"
                    "但不要凭空捏造画面里明显不存在的人和事。\n"
                    "3. 写成 3 到 4 段，每段两三句，起承转合自然，要有描写也要有感受。\n"
                    "4. 开头直接入景，不要「以下是一篇作文」「题目：」「作文：」这类套话，"
                    "也不要编号、标题、段落小标题。\n"
                    "5. 全文 300 到 450 字。\n"
                    "6. 这是要被音箱念出来的，句子别太长，读着顺口。\n"
                    "7. 全文用中文写，不要夹英文单词（音箱会一个字母一个字母念出来）。\n")
        return (head + common +
                "\n这次是**普通问答**：只说一到两句，口语化、简短，因为答案会被音箱念出来。\n")

    # 模型没答题、把任务说明或推理步骤吐出来了（agnes-2.5-flash 偶发）。
    # 实测两次：
    #   13:16「用户让我扮演家庭助理「小克」，唯一能力是看摄像头画面。用户问…」
    #   13:24「用户想要根据当前场景写一篇作文。 1. 分析画面内容： 、 时间：2026年10月4日…」
    # → 关键词表必然漏（第二次漏在"用户想要"），改用组合正则。
    # 这些都是正常口语回答里不会出现的东西，可以放心拦；
    # 误拦会导致静默不播报，所以只放高置信特征，不猜。
    META_RE = re.compile(
        r"用户(?:让我|问我|问：|想要|想让|希望|请求|需要|要求|说)"
        r"|让我扮演|我扮演|我的任务是|作为.{0,8}(?:助理|助手)"
        r"|分析画面|画面内容\s*[:：]|画面信息"
        r"|我(?:需要|要)(?:先|仔细)?(?:观察|分析)"
        r"|按(?:照)?(?:指定|规定|要求)的?格式"
        # ★ v0.1.4：「画面里能看到…」从这条里**摘掉了**。
        #   实测「客厅里有几个人」→ agnes 答「画面里能看到两个人。」被当复述任务拦掉，
        #   白换一轮模型（2.4s）。可这明明是个正常答案，只是口头上带了框架词。
        #   正确做法不是拦，是在 _clean 里把这个框架剥掉，见下。
        # ★ v0.1.4 日期规则：原来是裸的 `2026年10月4日`。写作文/日记时模型开头写日期
        #   再正常不过，会被当成"在列提纲"→ 换模型 → 全挂。改成必须带标签前缀。
        r"|(?:时间|日期|地点|人物|场景)\s*[:：]\s*\d")
    # 注意：这里**不再**放行首编号规则 —— 已挪到 _looks_meta，且只在整段开头判定。

    @classmethod
    def _looks_meta(cls, t):
        """判断模型是不是在复述任务/列提纲，而不是真的在作答。

        两步：先跑高置信特征正则，再看整段是不是以编号/项目符号起头。
        第二步必须只看**开头 20 字**：老版把 `^\\d+\\.` 塞进 re.M 的正则里，
        作文正文中间随便出现一个"1."（比如写"1. 妈妈在做饭"）就会把整篇判成提纲。
        """
        s = (t or "").strip()
        if not s:
            return False
        if cls.META_RE.search(s):
            return True
        return bool(re.match(r"^\s*(?:\d+[\.、)）]|[-*•·])\s*", s[:20]))

    # 长内容请求（作文/故事/总结）—— 这类天生要长输出，按普通规则限 80 字
    # 等于只念个开头就停（实测 13:24「此情此景你帮我写一篇作文」就是这个现象）。
    LONG_RE = re.compile(
        r"写(?:一|两|三|几)?(?:篇|个|段|首)?[^，。！？]{0,8}"
        r"(?:作文|文章|故事|日记|短文|诗|文案|总结|演讲稿|读后感)"
        r"|作文|讲(?:个|一个|一段)?故事|编(?:个|一个)?故事"
        r"|帮我写|替我写|详细(?:讲|说|介绍|说明)|展开(?:讲|说)")

    # ★ v0.1.4 续写：上一轮是长内容时，"小克，继续"要接着写，不是重写一篇。
    #   作文天生超过播报上限（450 字），念完必然停；用户想听后半段只能说"继续"。
    #   只看句首几个字，且必须 mode=wake + 上一轮是长内容 + 10 分钟内（见 compose_answer），
    #   否则电视里随便一句"继续"就会把小克接上去。
    CONTINUE_RE = re.compile(
        r"^\s*(?:(?:小\s*[克客柯科可刻课嗑恪溘哥])[，,。:：、\s]*)?"
        r"(?:继续|接着|往下|接下来|后面|还有呢|然后呢|再来一段|再写|再说下去|再讲下去"
        r"|继续说|说下去|讲下去|别停)")
    # 含这些词说明问的是"现在画面怎么样"，是新的视觉提问，不是续写
    CONT_BLOCK_RE = re.compile(r"几个人|几人|有没有人|在哪|什么颜色|认得|认识|手上|身上")

    def _llm(self, messages, max_tokens=None):
        llm = self.cfg["llm"]
        last_err = None
        for model in (llm.get("models") or []):
            try:
                body = {
                    "model": model,
                    "messages": messages,
                    "max_tokens": int(max_tokens or llm.get("max_tokens", 400)),
                    "temperature": llm.get("temperature", 0.7),
                }
                req = urllib.request.Request(
                    llm["base_url"].rstrip("/") + "/chat/completions",
                    data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                    headers={"Content-Type": "application/json",
                             "Authorization": "Bearer " + (llm.get("api_key") or "")},
                    method="POST")
                t0 = time.time()
                resp = urllib.request.urlopen(req, timeout=llm.get("timeout", 60),
                                              context=self.ctx)
                data = json.loads(resp.read().decode("utf-8", "replace"))
                content = (((data.get("choices") or [{}])[0].get("message") or {})
                           .get("content") or "").strip()
                if content and not self._looks_meta(content):
                    log.info("模型 %s 返回（%.1fs，路由 %s）", model, time.time() - t0,
                             resp.headers.get("X-Routed-Via"))
                    return content, model
                if content:
                    last_err = "复述任务说明"
                    log.warning("模型 %s 把任务说明复述回来了（%s），换下一个",
                                model, content[:36])
                else:
                    last_err = "空回复"
                    log.warning("模型 %s 空回复，换下一个", model)
            except urllib.error.HTTPError as e:
                last_err = "HTTP %s" % e.code
                log.warning("模型 %s 失败 %s，换下一个", model, last_err)
            except Exception as e:
                last_err = type(e).__name__
                log.warning("模型 %s 异常 %s，换下一个", model, e)
        log.error("所有模型都失败了（最后：%s）", last_err)
        return None, None

    def compose_answer(self, q, use_vision=True, mode=""):
        """抓帧 + 问模型，返回答案文本（失败返回 None）。"""
        long_req = bool(self.LONG_RE.search(q or ""))
        prev = self.last_task or {}
        cont_ok = (bool(self.CONTINUE_RE.match(q or ""))
                   and not self.CONT_BLOCK_RE.search(q or "")
                   and mode == "wake"
                   and prev.get("long")
                   and (time.time() - float(prev.get("at") or 0)) < self.cont_window)
        if cont_ok:
            long_req = True
            log.info("识别为续写，接上一轮「%s」的结尾往下写", (prev.get("q") or "")[:20])
        img = self.grab_frame() if use_vision else None
        if use_vision and not img:
            log.warning("取帧失败，降级为纯文本问答")
        if cont_ok:
            ask = ("你上一条按要求写了：「%s」。\n"
                   "你已经念出来的结尾是：「%s」\n"
                   "现在用户说：「%s」。\n"
                   "这是**接着往下写**：只写后面的新内容，不要重复念过的部分，"
                   "也不要重新开头。"
                   % (prev.get("q") or "", prev.get("tail") or "", q))
        else:
            ask = "用户刚才说：「%s」" % q
        # ★ 2026-10-19：把 Frigate 刚认到的人告诉模型。
        #   以前这段信息完全没用，模型只能从「两个孩子」里猜一个，
        #   两个孩子同框时会认反（10-18 那句「右边是姐姐」就是赌对的）。
        #   缓存里没有 5 分钟内的结果就主动查一次；60 秒内不重复试，别拖慢回答。
        if time.time() - self._fr_last_try > 60:
            self._fr_last_try = time.time()
            newest = max((t for t, _ in self.faces.values()), default=0)
            if time.time() - newest > 300:
                self.refresh_faces()
        faces = self.recent_faces()
        if faces:
            ask += ("\n（系统人脸识别刚刚认到：" + "、".join(faces) +
                    "。画面里的人就用这个名字，不要自己改名或猜。）")
            log.info("人脸先验：%s", "、".join(faces))
        if img:
            b64 = base64.b64encode(img).decode("ascii")
            log.info("取帧成功 %.1f KB", len(img) / 1024.0)
            cont = [{"type": "text", "text": ask},
                    {"type": "image_url",
                     "image_url": {"url": "data:image/jpeg;base64," + b64}}]
        else:
            cont = [{"type": "text", "text": ask}]
        ans, model = self._llm([{"role": "system",
                                 "content": self._system_prompt(long_req)},
                                {"role": "user", "content": cont}],
                               self.cfg["llm"].get("max_tokens_long") if long_req else None)
        if not ans:
            return None
        limit = int(self.cfg["trigger"].get(
            "long_max_chars" if long_req else "answer_max_chars", 150))
        if long_req:
            log.info("判定为长内容请求，播报上限放宽到 %d 字", limit)
        text = self._clean(ans, limit)
        # 记住这次长内容的尾巴，供"继续"接上下文
        if long_req and text:
            self.last_task = {"at": time.time(), "q": q, "long": True,
                              "tail": text[-300:]}
            self.save_state()
        return text

    @staticmethod
    def _clean(t, max_chars=80):
        """清掉模型爱加的前后缀，保证是能直接念出口的话。

        max_chars 是播报上限 —— 普通问答给 150 字足够（再长 TTS 念不完等于白答），
        作文/故事这类长内容请求会放宽（见 LONG_RE）。
        """
        t = t.strip()
        t = re.sub(r"^```.*?\n|\n```$", "", t, flags=re.S)
        t = re.sub(r"^(小克[:：]\s*)", "", t)
        # ★ v0.1.4：剥掉「画面里能看到…」这类框架词。
        #   实测「客厅里有几个人」→ agnes 答「画面里能看到两个人。」。
        #   prompt 里已经禁止，但模型偶尔还带；拦掉换模型白费一轮（2.4s），直接剥更划算。
        #   循环两轮，防「嗯，从画面里看…」这种叠加。
        for _ in range(2):
            t2 = re.sub(r"^(?:嗯|哦|好的|好)[，,、]?\s*", "", t)
            t2 = re.sub(r"^(?:从)?(?:画面|照片|图片|镜头|图)(?:里|中|上|里面)?"
                        r"(?:(?:可以|能|可)?(?:看得到|看到|看见)|可见|显示|有|是)?"
                        r"(?:的)?(?:话)?[，,：:]?\s*", "", t2)
            if t2 == t or len(t2.strip()) < 2:
                break
            t = t2
        for pre in ("好的，", "好的,", "嗯，", "啊，"):
            if t.startswith(pre) and len(t) > len(pre) + 4:
                t = t[len(pre):]
        # 小爱念不出 markdown 符号
        t = t.replace("**", "").replace("###", "").replace("* ", "、")
        # 只压空格/制表符，**保留换行** —— 长内容按段落念更自然
        t = re.sub(r"[ \t]+", " ", t)
        t = re.sub(r"\n{2,}", "\n", t).strip()
        # 超上限就截到最后一个句号
        if len(t) > max_chars:
            cut = max(t.rfind("。", 0, max_chars), t.rfind("！", 0, max_chars),
                      t.rfind("？", 0, max_chars), t.rfind("\n", 0, max_chars))
            t = t[:cut + 1] if cut > 20 else t[:max_chars] + "。"
        return t

    def speak_seconds(self, text):
        cps = float(self.cfg["trigger"].get("tts_chars_per_second", 4.5)) or 4.5
        return len(text or "") / cps

    def speak_chunks(self, text, chunk=None):
        """分段播报 —— 长内容一次塞进去会被念不完就覆盖掉。

        按句读切段，每段念完后按语速估算停顿再念下一段。
        """
        chunk = int(chunk or self.cfg["trigger"].get("speak_chunk_chars") or 80)
        segs, buf = [], ""
        for piece in re.split(r"(?<=[。！？；\n])", text or ""):
            if not piece.strip():
                continue
            if buf and len(buf) + len(piece) > chunk:
                segs.append(buf)
                buf = piece
            else:
                buf += piece
        if buf:
            segs.append(buf)
        # ★ v0.1.4：单段过长时按逗号再切。模型偶尔写超长句（一个句号前挂 200 字），
        #   按 chunk 切分只按句读做，这种句子会整段塞给 TTS → 念不完就被下一段盖掉。
        hard = int(chunk * 1.6)
        fixed = []
        for s in segs:
            while len(s) > hard:
                cut = max(s.rfind("，", 0, hard), s.rfind("；", 0, hard),
                          s.rfind("、", 0, hard), s.rfind(",", 0, hard))
                cut = cut if cut > 10 else hard
                fixed.append(s[:cut + 1])
                s = s[cut + 1:]
            if s:
                fixed.append(s)
        segs = fixed
        if len(segs) > 1:
            log.info("分段播报：%d 段，预计 %.0f 秒",
                     len(segs), sum(self.speak_seconds(s) for s in segs) + len(segs) * 0.6)
        ok = True
        for i, s in enumerate(segs):
            ok = self.ha.speak(s) and ok
            if i < len(segs) - 1:
                time.sleep(self.speak_seconds(s) + 0.6)
        return ok

    # -- 处理一条记录 -------------------------------------------------------
    def handle(self, rec, dry=False):
        q = (rec.get("query") or "").strip()
        if not q:
            return
        # 刚被管家当"回执"收下的话，别再当新问题答一遍
        if self.steward and self.steward.was_consumed(q):
            log.info("忽略「%s」（已被管家当回执收下）", q[:24])
            return
        max_len = int(self.cfg["trigger"].get("max_len", 120))
        if len(q) > max_len:
            log.info("忽略（太长 %d 字）：%s…", len(q), q[:30])
            return
        ans = self.answer_text(rec)
        ok, mode, why = self.judge(q, ans)
        if not ok:
            log.info("忽略「%s」（%s）", q[:40], why)
            return
        # 闸门。显式唤醒（wake）不受 min_gap 限制 —— 喊了就该答。
        # 实测 13:23:24 兴哥喊「小克客厅有多少个人」，被"距上次回答 1.2s"直接无视，
        # 用户感受就是"喊了不理"，比多播一次糟得多。wake 只保留 2 秒防抖。
        now = time.time()
        if mode == "wake":
            gap = float(self.cfg["trigger"].get("min_gap_wake_seconds", 2))
        else:
            gap = float(self.cfg["trigger"].get("min_gap_seconds", 8))
        if now - self.last_answer_at < gap:
            log.info("忽略「%s」（距上次回答 %.1fs < %.1fs）", q[:30],
                     now - self.last_answer_at, gap)
            return
        if self.answering:
            log.info("忽略「%s」（上一次还在处理）", q[:30])
            return
        if self._kaoguan_busy(now):
            log.info("忽略「%s」（考官正在说话，先让它）", q[:30])
            return
        log.info("🎯 响应[%s]「%s」（小爱答：%s）", mode, q, (ans or "—")[:40])
        t = threading.Thread(target=self._answer_job, args=(q, mode), daemon=True)
        t.start()

    def _kaoguan_busy(self, now):
        y = self.cfg.get("yield_to_kaoguan") or {}
        if not y.get("enabled") or not y.get("asking_quiet_seconds"):
            return False
        return (now - self.kaoguan_asking_at) < float(y["asking_quiet_seconds"])

    def log_memory(self, d):
        """Keke 侧的记忆落盘：wake / fill 的回答也要留痕。

        2026-10-19 新增。此前 log_event() 只在 Steward 的 say/reply 里调用，
        _answer_job() 答完直接 return —— 结果 10-18 17:24 那次
        「家里几个人 → 3个，你爸爸和两个女儿」+「爸爸在剥豆子，旁边还有垃圾桶」
        在 memory 里一个字都没有，第二天完全无从回忆。
        这里与 Steward.log_event 写同一个 memory/YYYY-MM-DD.jsonl，格式一致。
        """
        try:
            s = self.cfg.get("steward") or {}
            mem = s.get("memory_dir") or "/app/memory"
            os.makedirs(mem, exist_ok=True)
            p = os.path.join(mem, datetime.now().strftime("%Y-%m-%d") + ".jsonl")
            rec = dict(d, t=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as e:
            log.warning("写回答记忆失败：%s", e)

    def _answer_job(self, q, mode):
        self.answering = True
        t0 = time.time()
        try:
            ans = self.compose_answer(q, use_vision=True, mode=mode)
            if not ans:
                log.error("生成回答失败，这次跳过")
                return
            # 等小爱把「这可把我难住了」说完，否则小克会插进它半句话里
            wait = float(self.cfg["trigger"].get("speak_delay", 2.5))
            if wait > 0:
                time.sleep(wait)
            self.speak_chunks(ans)
            self.echoes.append(ans)
            self.last_answer_at = time.time()
            self.today_count += 1
            if mode == "fill":
                self.today_fill += 1
            self.save_state()
            # ★ 回答也进记忆（截断 600 字，长作文只留开头 + 全长）
            self.log_memory({"type": "answer", "mode": mode, "q": q,
                             "a": (ans or "")[:600], "a_len": len(ans or ""),
                             "cost_s": round(time.time() - t0, 1)})
            log.info("✅ 完成[%s] 总耗时 %.1fs：%s", mode, time.time() - t0, ans)
        except Exception as e:
            log.exception("处理失败：%s", e)
        finally:
            self.answering = False

    # -- 轮询 --------------------------------------------------------------
    def baseline(self):
        """启动时把现有记录全部标记为已见，免得一开机就把历史问句全答一遍。"""
        try:
            recs = self.mi.pull()
        except Exception as e:
            log.warning("建立基线失败（不影响启动）：%s", e)
            return
        for r in recs:
            rid = r.get("requestId") or str(r.get("time"))
            self.seen.add(rid)
            self.seen_order.append(rid)
            t = r.get("time") or 0
            if self.last_time is None or t > self.last_time:
                self.last_time = t
        log.info("基线已建立：%d 条历史记录，最后一条时间 %s", len(recs),
                 datetime.fromtimestamp((self.last_time or 0) / 1000.0).strftime("%H:%M:%S")
                 if self.last_time else "—")

    def poll(self):
        try:
            recs = self.mi.pull()
        except Exception as e:
            log.warning("拉取对话记录失败：%s", e)
            return
        new = []
        for r in recs:                      # 接口返回**最新在前**
            rid = r.get("requestId") or str(r.get("time"))
            if rid in self.seen:
                continue
            self.seen.add(rid)
            self.seen_order.append(rid)
            new.append((rid, r))
        for rid, r in reversed(new):        # 从旧到新依次处理
            t = r.get("time") or 0
            if self.last_time is not None and t < self.last_time:
                continue
            self.last_time = max(self.last_time or 0, t)
            self.handle(r)
        # 去重集合别无限长
        if len(self.seen_order) > 500:
            drop = self.seen_order[:300]
            self.seen_order = self.seen_order[300:]
            for d in drop:
                self.seen.discard(d)

    # -- MQTT（状态 + 远程指令 + 让路给考官）--------------------------------
    def start_mqtt(self):
        if mqtt is None:
            log.warning("没装 paho-mqtt，跳过 MQTT 通道（不影响主功能）")
            return
        m = self.cfg["mqtt"]
        try:
            self.mq = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=m.get("client_id") or "keke")
        except AttributeError:
            self.mq = mqtt.Client(client_id=m.get("client_id") or "keke")
        if m.get("username"):
            self.mq.username_pw_set(m["username"], m.get("password") or "")
        self.mq.on_connect = self._on_connect
        self.mq.on_message = self._on_message
        for attempt in range(1, 61):
            try:
                self.mq.connect(m["host"], m["port"], 60)
                self.mq.loop_start()
                return
            except Exception as e:
                log.warning("MQTT 连接失败（第 %d 次）：%s", attempt, e)
                time.sleep(10)
        log.error("MQTT 一直连不上，继续跑（只是没有远程通道）")

    def _on_connect(self, c, u, f, rc, props=None):
        try:
            c.subscribe([(MQTT_CMD_TOPIC, 0), ("kaoguan/state", 0),
                         ("frigate/events", 0)])
            log.info("MQTT 已连接 %s:%s", self.cfg["mqtt"]["host"], self.cfg["mqtt"]["port"])
        except Exception as e:
            log.warning("订阅失败：%s", e)
        try:
            self.publish_state(force=True)
        except Exception as e:
            log.warning("上线推状态失败：%s", e)

    def _on_message(self, c, u, msg):
        try:
            if msg.topic == MQTT_CMD_TOPIC:
                self._on_cmd(msg.payload.decode("utf-8", "replace").strip())
            elif msg.topic == "frigate/events":
                self._on_frigate_event(msg.payload)
            elif msg.topic == "kaoguan/state":
                d = json.loads(msg.payload.decode("utf-8", "replace"))
                self.kaoguan_state = d
                if d.get("asking"):
                    self.kaoguan_asking_at = time.time()
                la = d.get("last_ask")
                if la:
                    try:
                        today = datetime.now().strftime("%Y-%m-%d")
                        t = datetime.strptime("%s %s" % (today, la), "%Y-%m-%d %H:%M:%S")
                        self.kaoguan_asking_at = max(self.kaoguan_asking_at, t.timestamp())
                    except Exception:
                        pass
        except Exception as e:
            log.debug("处理 MQTT 消息失败：%s", e)

    def _on_frigate_event(self, payload):
        """从 Frigate 事件里抽人脸识别结果（sub_label），只做缓存。

        2026-10-19 新增：Frigate 的 face_recognition 一直在跑，认得出来是谁
        （实测近 6 小时 妹妹 25 次 / 姐姐 6 次，分数 0.87~1.0），
        但小克以前只订阅 keke/cmd + kaoguan/state，这些结果全浪费了，
        回答「这是谁」只能靠模型从「两个孩子」里猜。
        这个方法只解包、只存，任何异常都吞掉 —— 一条坏消息不能影响主循环。
        """
        try:
            d = json.loads(payload.decode("utf-8", "replace"))
            af = d.get("after") or {}
            name = (af.get("sub_label") or "").strip()
            if not name:
                return
            try:
                t = float(af.get("frame_time") or 0) or time.time()
            except Exception:
                t = time.time()
            # frame_time 应该是"此刻附近"的秒级时间；差太多说明字段口径变了，
            # 宁可退回当前时间，也不要拿一个错的时间去判断新鲜度。
            if abs(time.time() - t) > 600:
                t = time.time()
            score = af.get("sub_label_score")
            score = float(score) if score is not None else None
        except Exception:
            return
        self.faces[name] = (t, score)
        if len(self.faces) > 8:          # 只留最近几个，防字典无限涨
            for k in sorted(self.faces, key=lambda x: self.faces[x][0])[:-8]:
                self.faces.pop(k, None)

    def recent_faces(self, window=None):
        """最近 window 秒内认到的人，返回可直接拼进提示词的字符串列表。

        window 为什么给到 2 小时：Frigate 的 sub_label 是事件驱动的 —— 人坐着
        写作业不动，可能一小时才来一条（实测 18:42 认到姐姐，到 19:42 整整一小时
        没有新事件）。窗口按 30 分钟配的话，等于这功能白做。
        文案带「N 分钟前」，让模型自己判断新旧。
        """
        if window is None:
            window = float((self.cfg.get("frigate") or {}).get(
                "face_lookback_min") or 120) * 60
        now = time.time()
        out = []
        for name, (t, score) in sorted(self.faces.items(), key=lambda kv: -kv[1][0]):
            age = now - t
            if age > window:
                continue
            if age < 120:
                when = "刚刚"
            elif age < 3600:
                when = "%d 分钟前" % max(1, round(age / 60.0))
            else:
                when = "%d 小时前" % max(1, round(age / 3600.0))
            out.append("%s（%s%s）" % (name, when, "，置信 %.2f" % score if score else ""))
        return out

    def refresh_faces(self, limit=60):
        """主动问 Frigate：最近认到谁。

        ★ 为什么不能只靠 MQTT：sub_label 是事件驱动的，人坐着写作业不动，
        可能一小时才来一条。实测 19:37 问「这是谁」时 MQTT 缓存是空的，
        而 Frigate 的 /api/events 里 18:42 明明认到过姐姐。
        8971 是 HTTPS + cookie 登录（不是 basic auth），登录一次缓存 1 小时。
        """
        fg = self.cfg.get("frigate") or {}
        base = (fg.get("api") or "").rstrip("/")
        if not base or not fg.get("password"):
            return
        try:
            if (self._fr_op is None) or (time.time() - self._fr_login_at > 3600):
                self._fr_jar = http.cookiejar.CookieJar()
                self._fr_op = urllib.request.build_opener(
                    urllib.request.HTTPCookieProcessor(self._fr_jar),
                    urllib.request.HTTPSHandler(context=self.ctx))
                body = json.dumps({"user": fg.get("user") or "admin",
                                   "password": fg.get("password") or ""}).encode()
                req = urllib.request.Request(
                    base + "/api/login", data=body,
                    headers={"Content-Type": "application/json"})
                self._fr_op.open(req, timeout=6)
                self._fr_login_at = time.time()
                log.info("Frigate 登录成功，人脸查询已启用")
            r = self._fr_op.open(base + "/api/events?limit=%d" % limit, timeout=8)
            evs = json.loads(r.read().decode("utf-8", "replace"))
        except Exception as e:
            log.warning("查 Frigate 人脸失败（不影响回答）：%s", e)
            self._fr_op = None               # 下次重新登录
            return
        lookback = float(fg.get("face_lookback_min") or 30) * 60
        now = time.time()
        got = 0
        for e in evs or []:
            name = (e.get("sub_label") or "").strip()
            if not name:
                continue
            try:
                t = float(e.get("start_time") or 0)
            except Exception:
                continue
            if not t or now - t > lookback:
                continue
            score = e.get("sub_label_score")
            old = self.faces.get(name)
            if old and old[0] >= t:          # 已有更新的，不覆盖
                continue
            self.faces[name] = (t, float(score) if score is not None else None)
            got += 1
        if got:
            log.info("Frigate 人脸刷新：%s", "、".join(self.recent_faces()) or "无")

    def _on_cmd(self, cmd):
        low = cmd.lower()
        log.info("📨 收到远程指令：%s", cmd)
        if low in ("restart", "reload"):
            self.publish_state(force=True)
            os._exit(0)
        if low in ("status", "state"):
            self.publish_state(force=True)
            return
        if low.startswith("speak:"):
            self.ha.speak(cmd.split(":", 1)[1])
            return
        if low.startswith("ask:"):
            q = cmd.split(":", 1)[1]
            threading.Thread(target=self._answer_job, args=(q, "cmd"), daemon=True).start()
            return
        log.warning("不认识的指令：%s（可用 restart/status/speak:文本/ask:问题）", cmd)

    def _today_actions(self, limit=30):
        """★ v0.1.9：读今日事件日志，给看板用。返回按时间倒序的动作列表。

        数据源就是 Steward.log_event() 写的 memory/YYYY-MM-DD.jsonl —— 那边本来
        就在记，这里只是读出来放进 MQTT state，看板订阅 keke/state 就能直接显示，
        不用再开个 HTTP 接口或者让看板去读容器文件。

        倒序是因为看板要看"最近做了什么"，最新的排最上面。
        """
        mem = (self.cfg.get("steward") or {}).get("memory_dir")
        if not mem:
            return []
        try:
            p = os.path.join(mem, datetime.now().strftime("%Y-%m-%d") + ".jsonl")
            if not os.path.exists(p):
                return []
            out = []
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue          # 坏行跳过，别让一行垃圾毁掉整张卡
                    t = str(d.get("t") or "")[-8:-3]     # "HH:MM"
                    kind = d.get("type")
                    tag = d.get("tag") or ""
                    if kind in ("say", "reply"):
                        # 回话（reply）单独列 —— 它能暴露"答非所问"这类问题，
                        # 今天 18:00:38 就抓到过：问的是作业，答的是"我要听保林叔叔讲白蛇传"
                        out.append({"t": t, "kind": kind, "tag": tag,
                                    "text": (d.get("text") if kind == "say"
                                             else d.get("a") or "")[:40]})
            return out[-limit:][::-1]
        except Exception as e:
            log.debug("读今日动作失败：%s", e)
            return []

    def publish_state(self, force=False):
        if self.mq is None:
            return
        now = time.time()
        if not force and now - self._last_state_pub < 30:
            return
        self._last_state_pub = now
        payload = {
            "version": VERSION,
            "uptime_s": int(now - self.started_at),
            "today": self.today,
            "count": self.today_count,
            "fill_count": self.today_fill,
            "answering": self.answering,
            "last_answer": (datetime.fromtimestamp(self.last_answer_at).strftime("%H:%M:%S")
                            if self.last_answer_at else None),
            "kaoguan_asking": bool((now - self.kaoguan_asking_at)
                                   < float((self.cfg.get("yield_to_kaoguan") or {})
                                           .get("asking_quiet_seconds", 45))),
            "kaoguan_last_ask": (self.kaoguan_state or {}).get("last_ask"),
            "llm_models": (self.cfg.get("llm") or {}).get("models"),
            # ★ v0.1.9 今日动作流水，看板新卡片用。倒序（最新在最上）。
            "actions": self._today_actions(),
        }
        try:
            self.mq.publish(MQTT_STATE_TOPIC, json.dumps(payload, ensure_ascii=False),
                            retain=True)
        except Exception as e:
            log.debug("状态发布失败：%s", e)

    # -- 状态持久化 ---------------------------------------------------------
    def load_state(self):
        p = self.cfg.get("state_file")
        if not p or not os.path.exists(p):
            return
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
            if d.get("date") == self.today:
                self.today_count = d.get("count", 0)
                self.today_fill = d.get("fill_count", 0)
            # 续写上下文跟日期无关：跨天/重启后仍能接上（窗口比"继续"判据宽 3 倍）
            lt = d.get("last_task") or {}
            if lt.get("tail") and (time.time() - float(lt.get("at") or 0)) < self.cont_window * 3:
                self.last_task = lt
        except Exception as e:
            log.warning("读状态失败：%s", e)

    def save_state(self):
        p = self.cfg.get("state_file")
        if not p:
            return
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump({"date": self.today, "count": self.today_count,
                           "fill_count": self.today_fill,
                           "last_task": self.last_task or {}}, f,
                          ensure_ascii=False, indent=1)
        except Exception as e:
            log.warning("写状态失败：%s", e)

    # -- 主循环 -------------------------------------------------------------
    def run(self):
        # 耳朵凭据先主动验一次：缺了就明确报错，但别像以前那样直接把进程打死
        try:
            self.mi._ensure()
        except SystemExit as e:
            log.error("耳朵不可用（%s）—— 会继续跑，但收不到你说的话", e)
        self.start_mqtt()
        self.steward = Steward(self.cfg, self)
        log.info("小克 %s 已启动（dry-run=%s）；补位通道=%s；管家模式=%s；轮询 %.1fs；模型 %s",
                 VERSION, self.dry, "开" if self.cfg["trigger"].get("auto_fill") else "关",
                 "开" if self.steward.enabled else "关",
                 self.cfg["trigger"].get("poll_interval", 2.0),
                 ",".join((self.cfg.get("llm") or {}).get("models") or []))
        if self.steward.enabled:
            s = self.steward
            now0 = time.time()
            school0 = s._school_day(now0)
            win0 = s._fight_window(school0)
            log.info("管家作息：%s；静默 %s–%s；最小间隔 %.0f 秒",
                     "、".join("%s %s" % (x.get("person"), x.get("at"))
                               for x in (s.cfg_arrival.get("slots") or [])) or "—",
                     s.quiet[0], s.quiet[1], s.min_gap)
            # ★ 这行单独打出来：排障第一眼就是"今天走哪套班表"，别再翻配置猜。
            log.info("今日班表：%s（%s）；打闹 %s；假期问候 %s，一天≤%s 次",
                     "上学日" if school0 else "放假",
                     datetime.fromtimestamp(now0).strftime("%m-%d %a"),
                     ("%s–%s 每 %ss" % (win0["from"], win0["to"],
                                        win0["interval_s"])) if win0 else "不检测",
                     s.cfg_holiday_greet.get("mode") or "arrival",
                     s.cfg_holiday_greet.get("max_per_day"))
        self.baseline()
        interval = float(self.cfg["trigger"].get("poll_interval", 2.0))
        while True:
            time.sleep(interval)
            try:
                if datetime.now().strftime("%Y-%m-%d") != self.today:
                    self.today = datetime.now().strftime("%Y-%m-%d")
                    self.today_count = self.today_fill = 0
                    self.save_state()
                self.poll()
                self.publish_state()
                self.steward.tick()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                log.exception("主循环异常（继续跑）：%s", e)
                time.sleep(3)


# ============================================================================
# 管家模式：小克自己看时机开口
# ============================================================================
class Steward:
    """主动说话通道 —— 该写作业了、有人回来了、俩孩子在打闹。

    三条通道的分工：
      wake     用户喊"小克"           → 立即答
      fill     小爱认输 + 视觉意图词   → 补位答
      steward  本类：不等你开口，自己判断该不该说

    ⚠️ **耳朵只认音箱**（这条不认清，期望一定落空）：
       孩子在客厅随口回一句"做完了"，小克**听不到**。
       必须对着音箱说"小爱同学，做完了"才会进 HA 的 conversation 实体。
       所以主动提问本质是**单向提醒**，`reply_window_s` 那个回执窗口只是
       "孩子真答了就收下"。L05C 唤醒词烧在固件里，开不了连续对话。

    ⚠️ 主动说话 = 打扰人。所以有"同一件事当天只说一次"（fired）+ 最小间隔
       （min_gap_seconds）。这跟"不限次"不冲突：**不重复说**，但该说的照说。

    ⚠️ 耗时的抓帧/模型/播报都丢到后台线程（`busy` 互斥），否则会堵住主轮询 ——
       播报 90 秒 = 耳朵聋 90 秒。
    """

    GREET_SCHOOL = [
        "{who}回来啦，今天在学校怎么样啊？",
        "{who}到家啦，今天上课讲的内容都搞会了吗？",
        "回来啦{who}，今天学校里有啥好玩的事儿？",
        "{who}回来啦，先喝口水，今天学的东西都懂了吧？",
    ]
    GREET_HOLIDAY = [
        "今天玩得开心吗？",
        "放假在家呢，今天玩得开心嘛？",
        "今天过得怎么样啊，玩得开心不？",
    ]
    HOMEWORK = [
        "作业做完了吗？语文要背的都背好了没？",
        "作业写完了吗？今天记住几个英语单词了？",
        "作业做完了吧？语文背诵、英语单词都过一遍了吗？",
        "作业写完了吗？写完记得自己再检查一遍。",
    ]
    HOMEWORK_GAP = "作业写完了吗？昨天那道数学题，今天搞会了吧？"
    # ★ 2026-10-19：回执噪音识别。孩子对着音箱说的多数是「命令」而不是「回答」，
    #   实测 10-04~10-19 共 6 条回执：播放歌单六年级 / 关闭 / 我要听保林叔叔讲白蛇传
    #   / 声音调到25% / unavailable ×2 —— 没有一条是在回答小克。
    #   以前这些被原样记成 reply，导致「记忆」从源头就是脏的。
    #   现在只做**标记**（noise=true）不丢弃，方便回看；_yesterday_gap 会跳过它们。
    _NOISE_RE = re.compile(
        r"播放|放首|放个|歌单|音量|声音|调大|调小|关掉|关闭|关一下|静音|暂停|停止|"
        r"继续放|快进|下一首|上一首|小爱同学|唤醒|我要听|想听|给我听")
    # 实体异常值（10-16 17:41 那条回执就是 "unavailable"），同样不当回答
    _BAD_VALUES = {"unavailable", "unknown", "none", "null", "undefined", "error", "n/a"}

    BEDTIME = [
        "今天学到的东西，挑一个讲给爸爸妈妈听听呗。",
        "今天记住了几个英语单词？说三个听听。",
        "作业都检查过了吗？收拾收拾，早点睡。",
    ]
    FIGHT = [
        "哎哎，别闹了别闹了，小心磕着。",
        "两位小朋友，停一下，有话好好说，别动手。",
        "别追了别追了，摔一下多不划算。",
        "小声点小声点，别闹啦，去干点正事。",
    ]

    # ★ 看图问句必须"求格式"，但模型（尤其带思考的）爱先写分析再给结果，
    #   max_tokens 一卡，分析没写完正式答案就被截掉 → 解析出 0 人（预演时抓到）。
    #   所以：任务说明放 system 强调"禁止解释"，max_tokens 给 100 留余量，
    #   解析再加一层中文数字兜底；**解析不出来就返回 None 不做**，宁可不说也别乱说。
    #
    # ★ v0.1.8 改成多帧：会一次给 3 张**按时间先后**的图，措辞必须说清，
    #   否则模型把三张当三个不同场景各报一次人数（实测会出现 n=1/n=1/n=3）。
    LOOK_SYSTEM = ("你在看家庭摄像头的实时画面。回答会被程序直接解析，"
                   "所以只输出规定的两行：不许解释、不许分析、不许复述题目、"
                   "不许写思考过程、不许用英文。")
    LOOK_PROMPT = (
        "下面按时间先后给了同一台摄像头的几张画面，它们是**连续的同一片区域**。\n"
        "请综合判断，直接输出两行，第一个字就是「人」：\n"
        "人数=<数字>\n"
        "状态=<打闹|安静|无人>\n"
        "人数：这几帧里能看到几个人（取最多那帧的数），一个都没有就写 0。\n"
        "状态：几张画面里都在追逐、推搡、滚成一团才写「打闹」；\n"
        "      只是换个姿势、动一动、坐着玩 → 「安静」；没人 → 「无人」。\n"
        "拿不准就写「安静」。")

    def __init__(self, cfg, keke):
        s = cfg.get("steward") or {}
        self.cfg = s
        self.k = keke
        self.enabled = bool(s.get("enabled"))
        self.dry_run = bool(s.get("dry_run"))   # 只演练不开口
        self.holidays = set(s.get("holidays") or [])
        self.workdays = set(s.get("workdays") or [])   # 调休补班日（强制算上学日）
        self.min_gap = float(s.get("min_gap_seconds") or 150)
        self.reply_window = float(s.get("reply_window_s") or 90)
        self.quiet = s.get("quiet_hours") or ["21:45", "07:30"]
        self.cfg_arrival = s.get("arrival") or {}
        self.cfg_holiday_greet = s.get("holiday_greet") or {}
        self.cfg_homework = s.get("homework") or {}
        self.cfg_fight = s.get("fight") or {}
        self.cfg_bedtime = s.get("bedtime") or {}
        self.state_file = s.get("state_file") or "/app/steward-state.json"
        self.memory_dir = s.get("memory_dir") or "/app/memory"

        # HA 信号源
        self.ent_door = "sensor.lumi_wbmcn1_XXXX_lock_action"      # 门锁动作（outside_unlock=进门）
        self.ent_motion = "sensor.mi_XXXXXXXXX_message"            # 米家消息（门锁+摄像头共用单值实体）
        self.ent_fp = "sensor.lumi_wbmcn1_XXXX_lock_key_id"         # 指纹编号
        self.ent_fp_ts = "sensor.lumi_wbmcn1_XXXX_timestamp"       # 门锁设备侧时间戳
        self.conv_entity = "sensor.xiaomi_l05c_XXXX_conversation"  # 音箱实时对话内容

        self.ha_url = cfg["ha"]["url"]
        self.ha_headers = {"Authorization": "Bearer " + cfg["ha"]["token"]}
        self.ctx = keke.ctx if hasattr(keke, "ctx") else None

        self.fired = set()          # "YYYY-MM-DD|tag"
        self.pending = {}           # 等回话
        self.consumed = {}          # 已被当回执吃掉的话，防 handle 再答一遍
        self.last_say_at = 0.0
        self.last_fight_check = 0.0
        self.last_fight_say = 0.0
        self.last_holiday_greet = 0.0  # 假期进门问候上次开口时刻（限流用）
        self.fight_hits = 0        # 连续"打闹"判定次数，够了才开口
        self.arrived_at = 0.0
        self.busy = False
        self.fake_now = None       # 预演用：假装"现在"是某个别的时刻
        self._ts_cache = {}
        self._door_cache = None    # (查询时刻, 有无 outside_unlock)，15 秒 TTL
        self._msg_cache = None     # (查询时刻, 米家消息里有无门外开锁)，15 秒 TTL
        self._fp_cache = None      # (查询时刻, 门锁设备时间戳/指纹是否跳变)，20 秒 TTL
        # ★ 进门信号"新于上次处理"的水位线。存量型的信号源（查历史判断
        #   "最近 N 秒内有事件"）在重启/缓存失效后会把旧事件当新事件，
        #   实测 19:39 重启后 10 秒就误播了一次「今天过得怎么样」。
        #   构造完先取一次基线 = 当前时刻，之后只认更新的信号。
        self._arrival_floor_ts = time.time()
        # ★ v0.2.0：这次进门信号是从哪一路命中的、开门的人是谁。
        #   人名只对「消息」源有效（只有米家消息带"XX使用指纹门外开锁"），
        #   所以上层必须同时看 _arrival_src，不能只读人名 —— 否则消息源没命中、
        #   退到指纹/门锁源时，会拿上一次残留的人名去核对，把该报的也拦掉。
        self._arrival_src = None
        self._msg_arrival_person = None
        self.load()

    # -- 时间工具 ----------------------------------------------------------
    @staticmethod
    def _hhmm(s):
        try:
            h, m = str(s).split(":")
            return int(h) * 60 + int(m)
        except Exception:
            return 0

    @staticmethod
    def _now_min(now):
        d = datetime.fromtimestamp(now)
        return d.hour * 60 + d.minute

    def _in_window(self, now, at, window_min):
        """★ v0.1.8 修：原来用 `abs(差值) <= window_min`，是**双向对称**的 ——
        窗口会提前 window_min 分钟就开始命中。

        实测翻车：bedtime 配的是 20:45 / window 45，结果 **20:00:00 就播了**
        「今天学到的东西，挑一个讲给爸爸妈妈听听呗」—— 离 20:45 差了整整 45 分钟。
        同样受害的还有 arrival 的 16:40 / 17:40 两个槽位（16:05 就进窗口），
        会让"刚到家"的判断在半小时前就成立。

        语义上"从 at 开始、往后 window_min 分钟内"才是对的。
        跨零点的 at 不走这条路（那种该用 _in_span）。
        """
        d = self._now_min(now) - self._hhmm(at)
        return 0 <= d <= int(window_min)

    def _in_span(self, now, frm, to):
        return self._hhmm(frm) <= self._now_min(now) <= self._hhmm(to)

    def _fight_window(self, school):
        """挑打闹检测该用的时段 —— 上学日一套、放假另一套。

        ★ v0.1.6 新增。旧配置只有一个 from/to（16:00–21:00），是照"放学后"
          排的；国庆这种长假孩子全天在家，白天根本不在检测范围里。
          新格式（fight.workday / fight.holiday 各带 from/to/interval_s/cooldown_s）
          优先；没配就退回顶层的 from/to，老配置照样能跑。
          返回 None 表示这套时段没启用（放假不想检测就干脆不写 holiday 段）。
        """
        fc = self.cfg_fight
        w = fc.get("workday" if school else "holiday")
        if isinstance(w, dict):
            if w.get("from") is None or w.get("to") is None:
                return None
            return {"from": w["from"], "to": w["to"],
                    "interval_s": w.get("interval_s") or fc.get("interval_s") or 180,
                    "cooldown_s": w.get("cooldown_s") or fc.get("cooldown_s") or 1200}
        if fc.get("from") and fc.get("to"):
            return {"from": fc["from"], "to": fc["to"],
                    "interval_s": fc.get("interval_s") or 180,
                    "cooldown_s": fc.get("cooldown_s") or 1200}
        return None

    def _school_day(self, now):
        """上学日 = 工作日 且 不在 holidays 列表里。

        ★ v0.1.6：加 workdays（调休补班日）**强制**算上学日。
          中国调休制度下，周六周日可能是上课日（比如国庆前后那个周末补课），
          只按 weekday()>=5 判会把补课日误当放假 —— 那天孩子全天在校，
          小克却按假期班表走，上午九点半对着空客厅喊"今天玩得开心吗"。
          判定优先级：workdays > 周末 > holidays。
        """
        d = datetime.fromtimestamp(now)
        ds = d.strftime("%Y-%m-%d")
        if ds in self.workdays:
            return True
        if d.weekday() >= 5:
            return False
        return ds not in self.holidays

    def _arrived_today(self):
        """当天的"到家时刻"，跨天就作废。

        ★ v0.1.6：arrived_at 存在状态文件里、原来从不清。上学日 17:40 姐姐
          到家写了 arrived_at，第二天放假时作业追问会拿这个残留值算
          due = 17:40 + 45min = 18:25，而不是走 fallback 19:00 —— 时间飘了，
          而且放假孩子整天在家，"到家时刻"本来就没有意义。
        """
        if not self.arrived_at:
            return 0.0
        d = datetime.fromtimestamp(self.arrived_at).strftime("%Y-%m-%d")
        return self.arrived_at if d == self._today() else 0.0

    def _quiet_now(self, now):
        n = self._now_min(now)
        a, b = self._hhmm(self.quiet[0]), self._hhmm(self.quiet[1])
        return (a <= n <= b) if a <= b else (n >= a or n <= b)

    # -- 当天去重 ----------------------------------------------------------
    def _today(self, now=None):
        # fake_now 只在 --steward-dry 预演时被赋值（配 --date 假装是另一天）
        return datetime.fromtimestamp(now or self.fake_now or time.time()).strftime("%Y-%m-%d")

    def _count_today(self, tag):
        """今天这个 tag 说过几次。

        ★ v0.1.6：从"标记过没有"升级成"说过几次" —— 假期进门问候允许一天
          多次（上午出门玩、中午回来该问候；下午又出门、傍晚回来也该问候），
          光一个 bool 标记做不到。fired 里存的是 "日期|tag#序号"。
        """
        pre = self._today() + "|" + tag
        return sum(1 for k in self.fired
                   if k == pre or k.startswith(pre + "#"))

    def _fired_today(self, tag):
        return self._count_today(tag) > 0

    def _mark_today(self, tag):
        today = self._today()
        n = self._count_today(tag)
        self.fired = {k for k in self.fired if k.startswith(today)}   # 清掉旧的，别无限涨
        self.fired.add("%s|%s#%d" % (today, tag, n + 1))
        self.save()

    # -- HA 信号 -----------------------------------------------------------
    @staticmethod
    def _parse_ts(s):
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})", s or "")
        if not m:
            return None
        try:
            return datetime(*[int(x) for x in m.groups()], tzinfo=timezone.utc).timestamp()
        except Exception:
            return None

    def _fetch_last_change(self, eid, hours=2):
        start = datetime.fromtimestamp(time.time() - hours * 3600, timezone.utc)
        url = ("%s/api/history/period/%s?filter_entity_id=%s&minimal_response"
               % (self.ha_url, start.strftime("%Y-%m-%dT%H:%M:%S+00:00"), eid))
        try:
            req = urllib.request.Request(url, headers=self.ha_headers)
            data = json.loads(urllib.request.urlopen(req, timeout=12, context=self.ctx).read())
        except Exception as e:
            log.warning("管家查 %s 历史失败：%s", eid, e)
            return None
        if not data or not data[0]:
            return None
        last = data[0][-1]
        return self._parse_ts(last.get("last_changed") or last.get("last_updated"))

    def _last_change(self, eid, hours=2, ttl=20):
        """最近一次变化的时刻；带 20 秒缓存，免得每 2 秒打一次 HA。"""
        c = self._ts_cache.get(eid)
        if c and time.time() - c[0] < ttl:
            return c[1]
        t = self._fetch_last_change(eid, hours)
        self._ts_cache[eid] = (time.time(), t)
        return t

    def _state_value(self, eid):
        try:
            req = urllib.request.Request(self.ha_url + "/api/states/" + eid,
                                         headers=self.ha_headers)
            d = json.loads(urllib.request.urlopen(req, timeout=10, context=self.ctx).read())
            return str(d.get("state") or "")
        except Exception as e:
            log.warning("管家读 %s 失败：%s", eid, e)
            return ""

    def _arrival_signal(self, now, within_s=900, door_only=False):
        """最近有没有"有人进门"的迹象。

        door_only=True  → 只认门锁的 outside_unlock（"从外面开锁进门"），
                          查历史，不看当前状态。
        door_only=False → 门锁 outside_unlock 优先，没记到再退到摄像头
                          "有人移动"（小米每 10 分钟推一次，粒度粗）。
        无论哪种，都只是迹象，上层还得抓帧确认画面里真有人。

        ★★ 为什么必须查历史、不能读当前状态（2026-10-04 实锤）：
           真实动作序列是 outside_unlock（外面开门）→ 几秒后门锁自动
           lock_inside（上锁）。等你轮询到、去问 HA"现在是什么动作"，
           早就变成 lock_inside 了 —— 读当前状态必然漏掉每一次进门。
           （实测 24 小时里 outside_unlock 只挂了很短时间，历史里才有。）
        ★★ 为什么假期问候不能只靠摄像头（同日实测）：
           摄像头"有人移动"只说明客厅有人在动。孩子在家写一下午作业，
           这个信号一直是真，会被当成"反复进门"→ 一天问候三次。
           在家活动 ≠ 刚进门，所以假期必须 door_only=True。
        ★★★ 为什么需要"新于上次处理"这道闸（2026-10-04 19:39 实锤）：
           上面三个源都是**存量检查**——"最近 within_s 秒内有没有事件"。
           容器一重启、缓存一失效，函数立刻拿 15 分钟内的**旧事件**当新事件：
           实测 19:39:29 报「门锁设备时间戳跳变（38 秒前）」→ 10 秒后就播了
           「今天过得怎么样啊」，白烧掉一次 holiday_greet 额度。
           → 记录 `arrival_seen_at`，只认**比上次处理更新的**信号。
        """
        # 旧事件不算数：只在"确实有新事件"时返回 True
        if self._arrival_floor_ts and now - self._arrival_floor_ts < 60:
            return False
        # ★★ v0.2.0 换序：**消息实体排第一**（2026-11-04 全天对账后定的）。
        #   当天 5 次真实开门逐个核对三路信号：
        #     sensor.mi_XXXXXXXXX_message（消息，带人名）  5/5 全对，一分不差
        #     sensor.lumi_wbmcn1_XXXX_lock_action（动作）  2/5 —— 15:26/16:36/18:17
        #                                                  三次"门外开锁"都记成 lock_inside
        #     sensor.lumi_wbmcn1_XXXX_timestamp           单独不可用，须配侧信号
        #   旧顺序把最不可靠的 lock_action 放第一位、命中即 return，
        #   结果准确的那个源大多数时候**根本没被执行到**。
        #   现在按可靠性从高到低排，日志里能直接看到是哪一路命中的。
        for src, probe in (("消息", lambda: self._msg_arrival(now, within_s)),
                           ("指纹", lambda: self._fingerprint_arrival(now, within_s)),
                           ("门锁", lambda: self._door_arrival(now, within_s))):
            try:
                if probe():
                    log.info("管家：进门信号命中「%s」源", src)
                    self._arrival_src = src
                    self._arrival_floor_ts = now
                    return True
            except Exception as e:
                log.warning("管家门禁信号源异常(%s)：%s", src, e)
        # 摄像头兜底（仅非 door_only）
        if door_only:
            return False
        t = self._last_change(self.ent_motion, hours=1)
        # 0 <= 差值 <= within_s：负数说明拿的是"未来"的时间戳（预演用假时间跑的），
        # 不能算数 —— 之前没判非负，预演里打出过"进门迹象来自门锁（-10447 秒前）"。
        if t and 0 <= (now - t) <= within_s:
            log.info("管家：进门迹象来自摄像头（%.0f 秒前）", now - t)
            self._arrival_floor_ts = now
            return True
        return False

    # -- 门禁信号源 2：米家消息历史（含 prev_value） ------------------------
    #
    # ★★ 为什么必须扫历史、且必须看 prev_value（2026-10-04 17:07 实锤）：
    #    HA 里**只有** sensor.mi_XXXXXXXXX_message 一个实体承载**所有**门锁
    #    + 摄像头消息，后一条直接把前一条顶掉（单值实体，只能存一条）。
    #    对照手机米家 App 门锁日志 vs HA 收到的消息：
    #
    #      时刻   手机 App 真实事件            HA 实际收到
    #      16:09  奶奶 指纹**门外开锁**        16:09:25「门已关好」   ← 丢了
    #      17:07  妹妹 指纹**门外开锁**      17:07:25「门内上锁」   ← 丢了
    #      17:08  我 指纹**门外开锁**          17:08:40「门内上锁」   ← 丢了
    #      18:46  奶奶 指纹**门外开锁**        18:46:57 完整收到      ← 侥幸
    #
    #    「门外开锁」每次都被紧随其后的「门已关好 / 门内上锁」覆盖，
    #    **概率性丢包，不是延迟** —— 所以单靠读当前状态永远抓不到。
    #    但实体的 attributes 里有 prev_value：新事件来时会把被顶掉的那条
    #    留在 prev_value 里 → 扫历史时把 state + prev_value 都算进去，
    #    就能多救回一条。这也是为什么 16:09 那次在 scene_history 里能看到。
    def _msg_arrival(self, now, within_s=900):
        """米家消息历史里最近有没有「门外开锁」。含 prev_value 兜底。"""
        c = self._msg_cache
        if c and (time.time() - c[0]) < 15:
            return c[1]
        hit = False
        self._msg_arrival_person = None
        span = max(3600, int(within_s) + 600)
        start = datetime.fromtimestamp(time.time() - span, timezone.utc)
        url = ("%s/api/history/period/%s?filter_entity_id=%s&minimal_response"
               % (self.ha_url,
                  start.strftime("%Y-%m-%dT%H:%M:%S+00:00"), self.ent_motion))
        try:
            req = urllib.request.Request(url, headers=self.ha_headers)
            data = json.loads(urllib.request.urlopen(
                req, timeout=12, context=self.ctx).read())
            # ★ v0.2.0 改成**倒序**取最近一条。原来正序扫、命中即 break，
            #   拿到的是窗口内**最早**的那次开门：16:36 妹妹、16:38 奶奶
            #   两条都在窗口里时，只会读到 16:36 那条。人名核对要求"最新"。
            for p in reversed(data[0] if data else []):
                t = self._parse_ts(p.get("last_changed") or p.get("last_updated"))
                if not t or t > now or (now - t) > within_s:
                    continue
                texts = [str(p.get("state") or "")]
                # attributes 被 minimal_response 省掉了，prev_value 拿不到时
                # 至少 state 已经查过 —— 拿不到不算错，只是救不了这一条。
                texts.append(str((p.get("attributes") or {}).get("prev_value") or ""))
                for s in texts:
                    if "门外开锁" in s or "室外开门" in s:
                        # 剥掉设备前缀「小米智能门锁 Pro: 」，只留事件本身。
                        # ★ 原来用 s.split(":")[-1]，可事件里本来就带时间
                        #   （…姐姐 15:26使用指纹…），按冒号切会把正文拦腰砍断，
                        #   切剩「26使用指纹门外开锁」—— 人名直接没了。
                        body = re.sub(r"^[^:：]*[:：]\s*", "", s).strip()
                        hit = True
                        self._msg_arrival_person = self._person_from_msg(body)
                        log.info("管家：米家消息记录到「%s」（%.0f 秒前，人=%s）",
                                 body[:30], now - t, self._msg_arrival_person or "?")
                        break
                if hit:
                    break
        except Exception as e:
            log.warning("管家查米家消息历史失败：%s", e)
        self._msg_cache = (time.time(), hit)
        return hit

    # 「姐姐 15:26使用指纹门外开锁」→ 姐姐
    # ★★ 两个坑都踩过（2026-11-04 实测）：
    #   ① 时间戳夹在名字和"使用"之间，写成 ^(名字)(使用) 会把"使"一起吞进名字，
    #      实测抠出「姐姐使」。所以中间要允许 (?:HH:MM)?，且名字要**非贪婪**。
    #   ② 消息带设备前缀「小米智能门锁 Pro: 」，必须先剥前缀再匹配。
    #   用 search 而不是 match，前缀没剥干净时也能救回来。
    PERSON_FROM_MSG_RE = (r"([\u4e00-\u9fa5A-Za-z]{1,6}?)\s*"
                          r"(?:\d{1,2}[:：]\d{2})?\s*使用\s*指纹")
    # 这几个不是人名，是被正则误吞的主语
    _NOT_A_PERSON = ("有人", "门已", "门未", "门锁", "室外", "指纹", "密码", "钥匙")

    @classmethod
    def _person_from_msg(cls, s):
        """从米家消息里抠出开门的人。抠不到返回 None —— 上层按"不核对"处理。

        ★ 为什么需要它（2026-11-04）：窗口从 35 分钟放宽到 240 分钟后，
          窗口内**任何人**开门都会落进来。消息实体带人名，能核就核。
        """
        m = re.search(cls.PERSON_FROM_MSG_RE, (s or "").strip())
        if not m:
            return None
        who = m.group(1).strip()
        if not who or who in cls._NOT_A_PERSON:
            return None
        return who

    # -- 门禁信号源 3：指纹编号识别 ----------------------------------------
    #
    # ★ 场景：手机上明确是「妹妹 使用 右手大拇指 门外开锁」，指纹编号可查，
    #   但 HA 的 lock_action / lock_key_id 都没记下这次（lock_key_id 恒为 0）。
    #   那就退一步：**指纹编号本身**（左右拇指+人）变化 = 一次真实开门动作。
    #   在 HA 里查这个实体，谁用哪根手指开门是可追溯的，粒度到事件级。
    def _fingerprint_arrival(self, now, within_s=900):
        """最近有没有「真实开门动作」——**时间戳跳变 且 指纹编号/动作同时变**。

        ★ 为什么这个源能救回 17:07（2026-10-04 实锤）：
        #   手机米家 App 明确记着 17:07「妹妹 使用 右手大拇指 门外开锁」，
        #   但 HA 里 lock_action 那一轮**没记下**（云轮询漏了一次）。
        #   实测 17:08:42 三个实体同时刷新：
        #     lock_action  → lock_inside（没有 outside_unlock 这段）
        #     lock_key_id  → 0
        #     timestamp    → 09:08:39Z == 17:08:39（**设备侧真实事件时刻**）
        #   → 设备是**上报了**的，只是 lock_action 的单值状态被后一轮覆盖。
        #
        # ★★ 修正：单看 timestamp 会假阳性（2026-10-04 19:38 部署后实测）。
        #   门锁设备会**周期性上报心跳**（19:38:51 也跳了一次，那不是开门），
        #   容器一重启、缓存一失效就会当成"刚有人开门"→ 立刻误问候一次。
        #   教训：19:39:29 假阳性跳变 → 19:39:39 就播了「今天过得怎么样」，
        #   白烧掉一次 holiday_greet 额度。
        #   → 改成**双条件**：时间戳跳变 **且** 同一时间窗内
        #     lock_key_id 或 lock_action 也有变化，才算真开门。
        """
        c = self._fp_cache
        if c and (time.time() - c[0]) < 20:
            return c[1]
        hit = False
        span = max(3600, int(within_s) + 600)
        start = datetime.fromtimestamp(time.time() - span, timezone.utc)

        def hist(eid):
            url = ("%s/api/history/period/%s?filter_entity_id=%s&minimal_response"
                   % (self.ha_url,
                      start.strftime("%Y-%m-%dT%H:%M:%S+00:00"), eid))
            req = urllib.request.Request(url, headers=self.ha_headers)
            data = json.loads(urllib.request.urlopen(
                req, timeout=12, context=self.ctx).read())
            return [p for p in (data[0] if data else [])
                    if str(p.get("state") or "") not in
                    ("unknown", "unavailable", "None", "")]

        try:
            # 条件 1：设备时间戳在窗口内跳变
            ts_pts = hist(self.ent_fp_ts)
            ts_hit = False
            if len(ts_pts) >= 2:
                t_last = self._parse_ts(
                    ts_pts[-1].get("last_changed") or ts_pts[-1].get("last_updated"))
                t_prev = self._parse_ts(
                    ts_pts[0].get("last_changed") or ts_pts[0].get("last_updated"))
                ts_hit = bool(t_last and 0 <= (now - t_last) <= within_s
                              and t_prev and t_last and t_prev != t_last)
            # 条件 2：同窗口内指纹编号 / 门锁动作也变了（排除纯心跳）
            side_hit = False
            for eid in (self.ent_fp, self.ent_door):
                pts = hist(eid)
                if len(pts) >= 2:
                    a = str(pts[0].get("state") or "")
                    b = str(pts[-1].get("state") or "")
                    t_last = self._parse_ts(
                        pts[-1].get("last_changed") or pts[-1].get("last_updated"))
                    if not (a != b and t_last and 0 <= (now - t_last) <= within_s):
                        continue
                    # ★★ v0.2.0 收紧：门锁动作必须落在"门外"才算进门。
                    #   旧逻辑只看"变了"。17:40 实测：有人按门铃 → 门内开锁，
                    #   lock_action 从 lock_inside 变 inside_unlock，时间戳也跳，
                    #   两条都满足 → 被判成真实开门，于是报了"姐姐回来啦"。
                    #   门内开锁是真事件、但不是回家，必须按动作方向筛掉。
                    if eid == self.ent_door and "outside" not in b.lower():
                        log.info("管家：门锁动作变成 %s（不是门外），不算进门", b)
                        continue
                    log.info("管家：门锁侧信号 %s %s → %s", eid.rsplit("_", 1)[-1], a, b)
                    side_hit = True
                    break
            if ts_hit and side_hit:
                hit = True
                log.info("管家：门锁设备时间戳跳变 + 侧信号变化 → 判定为真实开门")
            elif ts_hit and not side_hit:
                log.info("管家：门锁时间戳跳了但侧信号没变，按心跳忽略"
                         "（避免重启后误问候）")
        except Exception as e:
            log.warning("管家查门锁设备信号失败：%s", e)
        self._fp_cache = (time.time(), hit)
        return hit

    def _door_arrival(self, now, within_s=900):
        """门锁历史里，最近 within_s 秒内有没有「从外面开门」（outside_unlock）。

        带 15 秒缓存 —— 主循环 2 秒一轮，别每次都打 HA 历史接口。
        """
        c = self._door_cache
        if c and (time.time() - c[0]) < 15:
            return c[1]
        # 查询窗口要**盖住 within_s**，否则窗口配得比 1 小时大时会静默失效
        # （实测：写死 3600 时 _door_arrival(now, 86400) 永远 False，
        #  因为 6 小时前那次 outside_unlock 压根不在返回结果里）。
        span = max(3600, int(within_s) + 600)
        start = datetime.fromtimestamp(time.time() - span, timezone.utc)
        url = ("%s/api/history/period/%s?filter_entity_id=%s&minimal_response"
               % (self.ha_url,
                  start.strftime("%Y-%m-%dT%H:%M:%S+00:00"), self.ent_door))
        hit = False
        try:
            req = urllib.request.Request(url, headers=self.ha_headers)
            data = json.loads(urllib.request.urlopen(
                req, timeout=12, context=self.ctx).read())
            pts = data[0] if data else []
            # ★★ v0.2.0 心跳抑制（2026-11-04 实锤）：这台门锁每 ~70 分钟上报一次
            #   `unavailable → outside_unlock`，当天实测 00:00 / 01:20 / 02:30 /
            #   03:40 / 04:50 各一次 —— 那不是有人开门，但 state 字面就是
            #   outside_unlock，旧逻辑照单全收。别以为只有半夜才吃亏：
            #   15 分钟窗口去撞 70 分钟一轮的心跳，命中率约 21%，
            #   等于五分之一的时间在说"有人进门"。
            #   真实开门的序列是 lock_inside → outside_unlock（上锁状态被开门打断），
            #   心跳的序列是 unavailable → outside_unlock。按"前一条状态"分开。
            prev_of = {}
            last_st = None
            for p in pts:
                st = str(p.get("state") or "").strip()
                prev_of[id(p)] = last_st
                if st:
                    last_st = st
            for p in reversed(pts):
                t = self._parse_ts(p.get("last_changed") or p.get("last_updated"))
                if not t or t > now:
                    continue          # 跳过"未来"时间戳（预演假时间）
                if (now - t) > within_s:
                    break             # 再往前就出了窗口，收工
                if "outside" not in str(p.get("state") or "").lower():
                    continue
                pv = (prev_of.get(id(p)) or "").strip()
                if pv.lower() in ("unavailable", "unknown", "none", ""):
                    log.info("管家：门锁 outside_unlock 的前一条是「%s」，按心跳忽略（%.0f 秒前）",
                             pv or "无", now - t)
                    continue
                hit = True
                log.info("管家：门锁记录到「从外面开门」（%.0f 秒前，前一条=%s）",
                         now - t, pv)
                break
        except Exception as e:
            log.warning("管家查门锁历史失败：%s", e)
        self._door_cache = (time.time(), hit)
        return hit

    # -- 看图 --------------------------------------------------------------
    # 中文 + 阿拉伯数字 + 中文数字 + 英文兜底。"没有"类判断放最后，避免抢先匹配
    PEOPLE_RE = (r"人数\s*[=＝:：]?\s*(\d+)"
                 r"|(\d+)\s*[个人位]"
                 r"|(\d+)\s*(?:people|person|persons)"
                 r"|([一二两三四五])\s*[个人位]")

    @classmethod
    def _parse_look(cls, ans):
        """从模型输出里抠人数和状态。

        ★ v0.1.8：**两个都必须抠到**才算成功。
          旧版只要抠到人数就返回，导致 state=""（日志里显示 "?"）也被当成功 ——
          实测 15 次里有 4 次这种"半个结果"。上层拿到 state="" 时，
          `look.get("state") != "打闹"` 判为不是打闹、什么都不做，倒不至于误报；
          但 `require_people` 之类只读人数的分支会拿着一个不可靠的数继续走。
          状态抠不到就当失败 → 换下一个模型 / 重试，比半吊子结果安全。
        """
        if not ans:
            return None
        n = None
        m = re.search(cls.PEOPLE_RE, ans, re.I)
        if m:
            g = m.groups()
            if g[0] is not None:
                n = int(g[0])
            elif g[1] is not None:
                n = int(g[1])
            elif g[2] is not None:
                n = int(g[2])
            elif g[3] is not None:
                n = "一二两三四五".index(g[3]) + 1
        if n is None and re.search(r"没有?人|无人|空无一人|一个人也没有|nobody|no one",
                                   ans, re.I):
            n = 0
        if n is None:
            return None
        st = ""
        for kw in ("打闹", "安静", "无人"):
            if kw in ans:
                st = kw
                break
        if not st:
            return None            # ★ v0.1.8 状态必须明确，不接受半个结果
        return {"n": n, "state": st}

    def look(self):
        """抓几帧问模型：几个人、在干什么。返回 {"n": int, "state": str} 或 None。

        ★ 不走 Keke._llm：那个只拦"复述/提纲"类**中文**输出，而看图这活儿模型爱吐
          **英文思考过程**（实测 "The user is asking me to analyze a security
          camera image. I need to ..."），照样溜得过去，最后解析出 0 人 →
          被上层当"客厅没人"，问题被藏起来。
          这里改成**解析成功才算通过**，抠不出人数/状态就换下一个模型重试 ——
          判据落在结果上，比拦关键词靠谱。

        ★ v0.1.8：多帧一次请求。同画面 15 次单帧实测「打闹 8 / 安静 2 / 空 4」，
          状态判定等于随机。改成把 3 帧**一起给模型让它自己综合**，
          耗时不变（一次请求），但模型能看到"这几帧是不是在动"这个关键信号。
        """
        frames = self.k.grab_frames(
            int(self.k.cfg.get("camera", {}).get("look_frames") or 3),
            float(self.k.cfg.get("camera", {}).get("look_frame_gap") or 1.5))
        if not frames:
            return None
        llm = self.k.cfg["llm"]
        # 文本在前、图在后：部分模型对"先给指令再给素材"的服从度更好
        content = [{"type": "text", "text": self.LOOK_PROMPT}]
        for f in frames:
            content.append({"type": "image_url",
                            "image_url": {"url": "data:image/jpeg;base64," +
                                          base64.b64encode(f).decode("ascii")}})
        last = None
        for model in (llm.get("models") or []):
            try:
                body = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": self.LOOK_SYSTEM},
                        {"role": "user", "content": content}],
                    "max_tokens": 150,          # 给"爱先思考"的模型留余量
                    "temperature": 0.1,
                }
                req = urllib.request.Request(
                    llm["base_url"].rstrip("/") + "/chat/completions",
                    data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                    headers={"Content-Type": "application/json",
                             "Authorization": "Bearer " + (llm.get("api_key") or "")},
                    method="POST")
                t0 = time.time()
                resp = urllib.request.urlopen(req, timeout=llm.get("timeout", 60),
                                              context=self.k.ctx)
                data = json.loads(resp.read().decode("utf-8", "replace"))
                ans = (((data.get("choices") or [{}])[0].get("message") or {})
                       .get("content") or "").strip()
                r = self._parse_look(ans)
                if r:
                    log.info("管家看图：人数=%s 状态=%s（%s，%d帧，%.1fs）",
                             r["n"], r["state"], model, len(frames), time.time() - t0)
                    return r
                last = ans
                log.warning("管家看图：%s 的输出抠不出人数/状态，换下一个 ← %s",
                            model, ans.replace("\n", " ")[:60])
            except Exception as e:
                last = str(e)
                log.warning("管家看图：%s 失败（%s），换下一个", model, e)
        log.error("管家看图：所有模型都没给出可用结果（最后：%s）", str(last)[:80])
        return None

    # -- 说话 --------------------------------------------------------------
    def say(self, text, tag, expect_reply=False):
        now = time.time()
        if self.k.answering:
            log.info("管家：小克正忙着，跳过[%s]", tag)
            return False
        if self.k._kaoguan_busy(now):
            log.info("管家：考官在说话，让路[%s]", tag)
            return False
        if now - self.last_say_at < self.min_gap:
            log.info("管家：距上次开口才 %.0f 秒，先不说[%s]", now - self.last_say_at, tag)
            return False
        log.info("🏠 管家开口[%s]：%s", tag, text)
        if self.dry_run:
            log.info("[dry] 只演练不播报（steward.dry_run=true）")
            ok = True
        else:
            ok = self.k.speak_chunks(text)
        self.last_say_at = time.time()
        if tag.startswith("arrival:"):
            self.arrived_at = self.last_say_at
        if tag == "holiday_greet":
            self.last_holiday_greet = self.last_say_at
        self.log_event({"type": "say", "tag": tag, "text": text})
        if expect_reply:
            self.pending = {"tag": tag, "text": text, "at": self.last_say_at,
                            "until": self.last_say_at + self.reply_window}
        self.save()
        return ok

    # -- 回执窗口 ----------------------------------------------------------
    def check_reply(self, now):
        """孩子对音箱说的话，如果落在提问后的窗口里，就当"回答"收下。"""
        p = self.pending or {}
        if not p:
            return
        if now > float(p.get("until") or 0):
            self.pending = {}
            self.save()
            return
        t = self._last_change(self.conv_entity, hours=1, ttl=6)
        if not t or t <= float(p.get("at") or 0):
            return
        val = self._state_value(self.conv_entity).strip()
        if not val or len(val) > 40 or self.k.is_wake(val) or self.k.is_echo(val):
            return
        # ★ 2026-10-19：先判是不是「对音箱下的命令」。是的话标 noise，仍记账但不参与
        #   「昨天欠账」判断，也不再回「好的，真棒」（孩子让关机却听到夸奖，很傻）。
        noise = (bool(self._NOISE_RE.search(val))
                 or val.strip().lower() in self._BAD_VALUES)
        log.info("📩 收到回话「%s」← 刚才问的是「%s」%s", val, p.get("text"),
                 "（指令，记 noise）" if noise else "")
        rec = {"type": "reply", "tag": p.get("tag"), "q": p.get("text"), "a": val}
        if noise:
            rec["noise"] = True
        self.log_event(rec)
        self.consumed[val] = time.time()      # 防止 handle() 再答一遍
        self.pending = {}
        self.save()
        if not noise and p.get("tag") in ("homework", "bedtime", "holiday_greet"):
            self.last_say_at = 0.0            # 应收一句就不受间隔限制
            self.say("好的，真棒。", "ack")

    def was_consumed(self, text, within_s=180):
        """这句话是不是刚被管家当回执收走了？是的话 handle() 就别再当新问题答。"""
        t = self.consumed.get((text or "").strip())
        if t and time.time() - t < within_s:
            return True
        # 顺手清老的
        now = time.time()
        self.consumed = {k: v for k, v in self.consumed.items() if now - v < within_s}
        return False

    # -- 昨天的账今天算 -----------------------------------------------------
    def _yesterday_gap(self):
        """昨天回话里有"还没/不会/忘了" → 今天换个问法接着追。"""
        p = os.path.join(self.memory_dir,
                         datetime.fromtimestamp(time.time() - 86400).strftime("%Y-%m-%d") + ".jsonl")
        if not os.path.exists(p):
            return False
        try:
            for line in open(p, encoding="utf-8"):
                d = json.loads(line)
                if (d.get("type") == "reply" and not d.get("noise") and re.search(
                        r"还没|没有|不会|没会|没做|忘了|没写", d.get("a") or "")):
                    return True
        except Exception:
            pass
        return False

    # -- 记忆 --------------------------------------------------------------
    def log_event(self, d):
        d = dict(d, t=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        try:
            os.makedirs(self.memory_dir, exist_ok=True)
            p = os.path.join(self.memory_dir,
                             datetime.now().strftime("%Y-%m-%d") + ".jsonl")
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(d, ensure_ascii=False) + "\n")
        except Exception as e:
            log.warning("写管家日志失败：%s", e)

    # -- 状态持久化 --------------------------------------------------------
    def save(self):
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump({"fired": sorted(self.fired),
                           "last_say_at": self.last_say_at,
                           "arrived_at": self.arrived_at,
                           "last_holiday_greet": self.last_holiday_greet,
                           "pending": self.pending}, f, ensure_ascii=False, indent=1)
        except Exception as e:
            log.warning("写管家状态失败：%s", e)

    def load(self):
        try:
            if os.path.exists(self.state_file):
                d = json.load(open(self.state_file, encoding="utf-8"))
                self.fired = set(d.get("fired") or [])
                self.last_say_at = float(d.get("last_say_at") or 0)
                self.arrived_at = float(d.get("arrived_at") or 0)
                self.last_holiday_greet = float(d.get("last_holiday_greet") or 0)
                self.pending = d.get("pending") or {}
        except Exception as e:
            log.warning("读管家状态失败：%s", e)

    # -- 决策 --------------------------------------------------------------
    def _decide(self, now):
        school = self._school_day(now)

        # ① 放学回家（上学日）
        if school and self.cfg_arrival.get("enabled"):
            for slot in (self.cfg_arrival.get("slots") or []):
                tag = "arrival:%s" % (slot.get("person") or "?")
                if self._fired_today(tag):
                    continue
                if not self._in_window(now, slot.get("at"), slot.get("window_min") or 30):
                    continue
                # ★★ v0.2.0 修两处（2026-11-04 复盘）：
                #   ① 必须 door_only=True。原来用的是默认 False，探测失败会退到
                #      摄像头"有人移动"兜底 —— 17:40 那次就是靠 454 秒前的一个
                #      人影，报出了"姐姐回来啦"。依据是假的，只是碰巧人真回来了。
                #      假期问候那段一直是传 door_only 的，上学日这段漏了。
                #   ② 进门的是谁要核对（见下）。
                if not self._arrival_signal(now, door_only=True):
                    continue
                # 窗口放宽后，窗口内**任何人**开门都会落进来 —— 今天就有奶奶
                # 15:34、爸爸 18:17 各一次。不核对会对着奶奶喊"姐姐回来啦"。
                # 只有「消息」源带人名，抠到了就核，抠不到（或走的别的源）按旧逻辑放行。
                who = (self._msg_arrival_person
                       if self._arrival_src == "消息" else None)
                if who and slot.get("person") and who != slot.get("person"):
                    log.info("管家：这次开门的是「%s」，不是「%s」，跳过 %s",
                             who, slot.get("person"), tag)
                    continue
                return {"tag": tag, "need_look": True, "require_people": True,
                        "expect_reply": True,
                        "text": random.choice(self.GREET_SCHOOL).format(
                            who=slot.get("person") or "宝贝")}

        # ② 作业追问 —— 到家后 delay 分钟；没探到"到家"就退到 fallback_at
        #
        # ★ v0.1.6 顺序坑：作业必须排在**假期问候之前**。放假时孩子常常晚饭前
        #   才从外面回来，19:00 左右正好是"刚进门"（假期问候想说话）和"该问作业"
        #   （fallback 19:00）撞在一起 —— 假期问候排前面的话，它会一天说 3 次、
        #   把作业追问整晚挤没（预演 19:00 抓到的）。
        hw = self.cfg_homework
        if hw.get("enabled") and not self._fired_today("homework"):
            arrived = self._arrived_today()      # 跨天作废，见 _arrived_today
            if arrived:
                due = self._now_min(arrived) + int(
                    hw.get("delay_after_arrival_min") or 45)
            else:
                due = self._hhmm(hw.get("fallback_at") or "19:00")
            if abs(self._now_min(now) - due) <= int(hw.get("window_min") or 60):
                return {"tag": "homework", "need_look": True, "require_people": True,
                        "expect_reply": True,
                        "text": (self.HOMEWORK_GAP if self._yesterday_gap()
                                 else random.choice(self.HOMEWORK))}

        # ③ 放假在家的问候
        #
        # ★ v0.1.6 重做：原来只有"09:30 问一次"，可放假期孩子一大早就在家了，
        #   09:30 之后一整天（到 20:45 睡前）小克再没说过话。
        #   兴哥原话"如果是放假时间回到家 就问今天玩的开心嘛" —— 假期孩子是
        #   **进进出出**的（上午出去玩、中午回来吃饭、下午又出去），
        #   问候就该挂在"进门"这个动作上，而不是钉死在某个钟点。
        #   所以主模式改成 arrival：每次检测到进门迹象就问一句，一天最多
        #   max_per_day 次、两次之间至少隔 min_gap_min 分钟。
        hg = self.cfg_holiday_greet
        if (not school) and hg.get("enabled"):
            mode = str(hg.get("mode") or "arrival").lower()
            if mode == "arrival":
                maxn = int(hg.get("max_per_day") or 3)
                gap = int(hg.get("min_gap_min") or 90) * 60
                if (self._in_span(now, hg.get("active_from") or "08:30",
                                  hg.get("active_to") or "20:00")
                        and self._count_today("holiday_greet") < maxn
                        and (now - self.last_holiday_greet) >= gap
                        and self._arrival_signal(
                            now, within_s=int(hg.get("arrival_within_s") or 900),
                            door_only=bool(hg.get("door_only", True)))):
                    return {"tag": "holiday_greet", "need_look": True,
                            "require_people": True, "expect_reply": True,
                            "text": random.choice(self.GREET_HOLIDAY)}
            else:
                # clock 模式：保留旧的固定时间点问候
                if (not self._fired_today("holiday_greet")
                        and self._in_window(now, hg.get("at"),
                                            hg.get("window_min") or 120)
                        and self._arrival_signal(now, within_s=1800)):
                    return {"tag": "holiday_greet", "need_look": True,
                            "require_people": True, "expect_reply": True,
                            "text": random.choice(self.GREET_HOLIDAY)}

        # ④ 睡前收尾（不抓帧 —— 这个点孩子可能在房间，画面里没人不代表不在家）
        # ★ v0.1.5 顺序坑：睡前必须排在**打闹之前**。打闹时段覆盖 20:45，
        #   睡前原本排在打闹后面，20:45 永远被"该查打闹了"抢走，
        #   睡前提问整晚都触发不了（预演时抓到的）。
        bt = self.cfg_bedtime
        if bt.get("enabled") and not self._fired_today("bedtime"):
            if self._in_window(now, bt.get("at"), bt.get("window_min") or 45):
                return {"tag": "bedtime", "expect_reply": True,
                        "text": random.choice(self.BEDTIME)}

        # ⑤ 打闹提醒（可重复，但有冷却）
        #   ★ v0.1.6：时段按上学日/放假分两套。原来写死 16:00–21:00 是照"放学后"
        #     排的 —— 国庆孩子全天在家，白天闹翻天它一声不吭（兴哥 10-04 指出的）。
        fc = self.cfg_fight
        win = self._fight_window(school)
        if fc.get("enabled") and win and self._in_span(now, win["from"], win["to"]):
            if (now - self.last_fight_check >= int(win.get("interval_s") or 180)
                    and now - self.last_fight_say >= int(win.get("cooldown_s") or 1200)):
                # ★ 省一笔：客厅明显没人动就别抓帧问模型了。
                #   摄像头"有人移动"是 10 分钟粒度的推送，15 分钟内没有过就跳过。
                #   不然白天没人时每 3 分钟白跑一次模型（一天上百次）。
                mt = self._last_change(self.ent_motion, hours=1)
                if not mt or (now - mt) > 900:
                    self.last_fight_check = now
                    return None
                self.last_fight_check = now
                return {"tag": "fight", "need_look": True, "require_fight": True,
                        "once_per_day": False, "text": random.choice(self.FIGHT)}

        return None

    def _run_action(self, act):
        try:
            if act.get("need_look"):
                look = self.look()
                if not look:
                    log.info("管家：看图失败，这次不动[%s]", act.get("tag"))
                    return
                if act.get("require_people") and (look.get("n") or 0) <= 0:
                    log.info("管家：画面里没人，不开口[%s]", act.get("tag"))
                    return
                if act.get("require_fight") and look.get("state") != "打闹":
                    return
                if act.get("require_fight"):
                    # ★ 必须**连续确认**才开口。实测同一场景连问三次，模型给出的是
                    #   「3人打闹 / 1人 / 2人安静」—— 单帧判定根本不能信，
                    #   误报一次就是对着安静写作业的孩子喊"别闹了"，很烦人。
                    #
                    # ★ v0.1.7 人数下限改成可配（2026-10-04 实测 23 次触发 0 次开口）：
                    #   原来硬编码 `n >= 2`。而看图本身极不稳 —— 实测同一天里
                    #   「人数=1 状态=打闹」「人数=0 状态=打闹」反复出现（人数甚至
                    #   从模型输出里抠不出来，70 次里有 7 次失败换模型重试）。
                    #   → fight_hits 恒为 0，一整天 0 次开口，功能等于没开。
                    #   现在下限读配置 min_people（默认 2），
                    #   并可选 trust_state_when_no_people：人数读数不可靠时，
                    #   只要模型明确说"打闹"就信。
                    #
                    # ★ v0.1.8 上游已改成多帧联合判定（3 帧一次请求），
                    #   单帧状态噪声（打闹 8/15、安静 2/15、空 4/15）已从源头压掉。
                    #   但**人数=1 时模型仍会说"打闹"**（一个人玩闹也算），
                    #   所以 min_people=2 这道闸必须保留 —— 它现在是唯一的硬防线。
                    _n = look.get("n")
                    _min_people = int(self.cfg_fight.get("min_people") or 2)
                    _is_fight = (look.get("state") == "打闹")
                    if _is_fight and (_n is None or _n <= 0):
                        # 人数读不出来（None / 0）——按配置决定信不信"打闹"
                        if self.cfg_fight.get("trust_state_when_no_people"):
                            _n = _min_people
                        else:
                            _n = -1          # 强制不累加，但打日志便于调参
                    if _is_fight and _n >= _min_people:
                        self.fight_hits += 1
                    else:
                        if _is_fight:
                            log.info("管家：判打闹但人数不足（n=%s < %d），不计确认",
                                     _n, _min_people)
                        # ★ v0.1.8：状态判为"安静"时**不重置** fight_hits。
                        #   原逻辑只要本帧不是打闹就归零，可打闹检测间隔 300 秒、
                        #   孩子玩闹中间必然有安静的帧 → 计数永远攒不到 2，恒为 0。
                        #   改成：明确看到 ≥min_people 人在场但不闹 → 才归零。
                        elif _n is not None and _n >= _min_people:
                            self.fight_hits = 0
                    need = int(self.cfg_fight.get("confirm_times") or 2)
                    if self.fight_hits < need:
                        log.info("管家：疑似打闹（第 %d/%d 次确认），先不开口",
                                 self.fight_hits, need)
                        return
                    self.fight_hits = 0
                    self.last_fight_say = time.time()
            if act.get("once_per_day", True):
                self._mark_today(act["tag"])
            self.say(act.get("text"), act["tag"], act.get("expect_reply"))
        except Exception as e:
            log.exception("管家动作失败：%s", e)
        finally:
            self.busy = False

    def tick(self, now=None):
        now = now or time.time()
        if not self.enabled or self.busy:
            return
        try:
            self.check_reply(now)
        except Exception as e:
            log.warning("管家回执检查失败：%s", e)
        if self.k.answering or self._quiet_now(now):
            return
        if now - self.last_say_at < self.min_gap or self.k._kaoguan_busy(now):
            return
        try:
            act = self._decide(now)
        except Exception as e:
            log.exception("管家决策失败：%s", e)
            return
        if not act:
            return
        self.busy = True
        threading.Thread(target=self._run_action, args=(act,), daemon=True).start()


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(BASE, "keke-config.yaml"))
    ap.add_argument("--dry-run", action="store_true", help="只判定不播报")
    ap.add_argument("--test-pull", action="store_true", help="拉一次对话记录并打印")
    ap.add_argument("--test-ask", metavar="Q", help="走完整链路（抓帧+模型），不播报")
    ap.add_argument("--test-speak", metavar="TEXT", help="只测播报通道")
    ap.add_argument("--judge", metavar="Q", help="只做判定，不联网（调规则用）")
    ap.add_argument("--steward-dry", action="store_true",
                    help="只跑管家决策，不播报（配 --at 可假装时间）")
    ap.add_argument("--at", metavar="HH:MM", help="配 --steward-dry：假装现在是这个点")
    ap.add_argument("--date", metavar="YYYY-MM-DD",
                    help="配 --steward-dry：假装今天是这天（试上学日/放假班表）")
    ap.add_argument("--fake-arrival", action="store_true",
                    help="配 --steward-dry：假装「刚有人进门」（预演时假时间戳算不出进门迹象）")
    ap.add_argument("--fake-who", metavar="NAME",
                    help="配 --fake-arrival：假装开门的是这个人（验证人名校验闸）")
    ap.add_argument("--test-look", action="store_true", help="只测管家看图（人数/状态）")
    ap.add_argument("--answers", default="", help="配合 --judge：假装小爱的回复是这句")
    ap.add_argument("--state", action="store_true", help="打印生效配置（不含密钥）")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    cfg = load_cfg(args.config)

    if args.state:
        llm = cfg.get("llm") or {}
        print("版本      : %s" % VERSION)
        print("HA        : %s" % cfg["ha"]["url"])
        print("播报实体  : %s" % cfg["ha"]["tts_entity"])
        print("取帧地址  : %s" % (cfg.get("camera") or {}).get("snapshot_url"))
        print("模型网关  : %s" % llm.get("base_url"))
        print("模型      : %s" % ",".join(llm.get("models") or []))
        print("小米 token: %s" % (cfg.get("mi") or {}).get("token_path"))
        print("唤醒字表  : %s" % cfg["wake"].get("chars"))
        print("补位通道  : %s" % ("开" if cfg["trigger"].get("auto_fill") else "关"))
        print("认输话术  : %d 条" % len(cfg.get("surrender_phrases") or []))
        print("视觉意图词: %d 个" % len(cfg.get("vision_intent") or []))
        print("轮询间隔  : %ss" % cfg["trigger"].get("poll_interval"))
        sys.exit(0)

    k = Keke(cfg, dry=args.dry_run)

    if args.test_speak:
        sys.exit(0 if k.ha.speak(args.test_speak) else 1)

    if args.test_look or args.steward_dry:
        k.steward = Steward(cfg, k)
        if args.test_look:
            print("看图结果：%s" % (k.steward.look() or "❌ 失败"))
            sys.exit(0)
        now = time.time()
        if args.at:
            hh, mm = args.at.split(":")
            now = datetime.now().replace(hour=int(hh), minute=int(mm),
                                         second=0, microsecond=0).timestamp()
        if args.date:
            y, mo, d = [int(x) for x in args.date.split("-")]
            now = datetime.fromtimestamp(now).replace(
                year=y, month=mo, day=d).timestamp()
        s = k.steward
        s.fake_now = now                      # 让 _today()/_count_today() 也认假日期
        if args.fake_arrival:
            # 预演时 now 是"假装的时间"，跟 HA 里的真实时间戳差好几天，
            # _arrival_signal 永远算不出"刚进门"。这里直接盖掉，好验证问候链路。
            # ★ v0.2.0：签名必须带上 door_only —— 上学日的 arrival 分支现在传它了，
            #   漏掉会 TypeError（got an unexpected keyword argument）。
            s._arrival_signal = lambda now, within_s=900, door_only=False: True
            # 默认按"消息源命中"处理。名字给定时才做核对 —— 传空串表示
            # "信号有、但抠不出人名"，用来验证兜底放行那条路。
            if args.fake_who is not None:
                s._arrival_src = "消息"
                s._msg_arrival_person = args.fake_who or None
            else:
                s._arrival_src = "假"
        school = s._school_day(now)
        win = s._fight_window(school)
        print("管家模式  : %s" % ("开" if s.enabled else "关"))
        print("假装此刻  : %s" % datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M %a"))
        print("上学日    : %s" % school)
        print("打闹时段  : %s" % ("%s–%s，每 %ss 看一眼，冷却 %ss"
                                 % (win["from"], win["to"], win["interval_s"],
                                    win["cooldown_s"]) if win else "本日不检测"))
        print("静默时段  : %s" % s._quiet_now(now))
        print("今日已说  : %s" % (sorted(x.split("|")[-1] for x in s.fired) or "—"))
        print("进门迹象  : %s" % s._arrival_signal(now))
        print("昨天的账  : %s" % ("有欠账（会换问法追）" if s._yesterday_gap() else "无"))
        act = s._decide(now)
        print("-" * 60)
        if not act:
            print("决策：此刻不该说话")
        else:
            print("决策：说「%s」（tag=%s，要抓帧确认=%s）"
                  % (act.get("text"), act.get("tag"), bool(act.get("need_look"))))
            if act.get("need_look"):
                print("看图：%s" % (s.look() or "❌ 失败"))
        sys.exit(0)

    if args.judge:
        ok, mode, why = k.judge(args.judge, args.answers)
        print("输入问句 : %s" % args.judge)
        print("小爱回复 : %s" % (args.answers or "（空）"))
        print("命中唤醒 : %s" % k.is_wake(args.judge))
        print("小爱认输 : %s" % k.is_surrender(args.answers))
        print("视觉意图 : %s" % k.has_vision_intent(args.judge))
        print("结论     : %s%s（%s）" % ("✅ 响应" if ok else "❌ 不响应",
                                        "  [%s]" % mode if mode else "", why))
        sys.exit(0 if ok else 1)

    if args.test_pull:
        try:
            recs = k.mi.pull()
        except Exception as e:
            print("拉取失败：%s" % e)
            sys.exit(1)
        print("拉到 %d 条（最新在前）：" % len(recs))
        for r in recs:
            t = r.get("time") or 0
            when = (datetime.fromtimestamp(t / 1000.0).strftime("%m-%d %H:%M:%S")
                    if t else "—")
            ans = k.answer_text(r)
            print("  [%s] Q=%s" % (when, r.get("query")))
            print("            A=%s" % (ans[:60] or "（无）"))
            ok, mode, why = k.judge(r.get("query") or "", ans)
            print("            判定：%s（%s）" % ("响应[%s]" % mode if ok else "忽略", why))
        sys.exit(0)

    if args.test_ask:
        ans = k.compose_answer(args.test_ask, use_vision=True, mode="wake")
        print("问题：%s" % args.test_ask)
        print("回答：%s" % (ans or "❌ 失败"))
        if k.last_task:
            print("（已记住长内容尾巴 %d 字，可供「继续」接上下文）"
                  % len(k.last_task.get("tail") or ""))
        sys.exit(0 if ans else 1)

    k.run()


if __name__ == "__main__":
    main()
