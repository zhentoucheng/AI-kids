# -*- coding: utf-8 -*-
"""
家庭小考官 v0.4.1  ——  门锁 × 监控 × 视觉大模型 联动抽考

两路信号怎么合起来用
─────────────────────────────────────────────────────────────
  信号 A  门锁  sensor.lumi_wbmcn1_XXXX_lock_key_id   → 今天谁回过家
  信号 B  监控  frigate/<zone>/<label>/active         → 客厅此刻几个人（整数）
  信号 C  人脸  frigate/events 里 after.sub_label     → 认出来的那个人是谁（可选）

判定链
  客厅人数≥N 持续 hold_seconds 秒
  → 再过时段/间隔/次数三道闸 → 随机延迟 0~random_delay_max 秒 → 小爱音箱出题
  → answer_delay 秒后自动报答案（英文答案还会念拼写）

★ v0.4.0 新增：三种出题方式（由 config.quiz 控制比例）
  ① 看画面出题（vision_ratio）—— 真正"结合实际场景"的那条路：
       抓一张摄像头快照 → 连同**该生年级的词表**一起喂给视觉大模型
       → 模型先描述画面里孩子在做什么、身边有什么，再从词表里挑一个搭得上的词
       → 出题：「我看见{场景}。{谁}，你身边的『{中文}』用英语怎么说？」
     模型不可用/超时 → 自动落回 ②，保证"不定期出题"不中断。
  ② 年级词表抽词（words_ratio）—— 从 words/<年级>.txt 随机抽一个词
  ③ questions.yaml 题库 —— 兜底

★ 和门锁自动化的关系：并行，不冲突
  门锁那条「孩子回家自动播英语」是**独立**的 HA 自动化（门外开锁→25s→开口白→
  8s→播六年级/二年级歌单），kaoguan 不去动它、也不靠它。
  唯一要错开的是"谁在说话"：门口一有开锁，kaoguan 就安静
  lock_parallel.arrival_quiet_minutes 分钟，让英语先播完，然后再接着考。
  故意**不**拿"小爱正在播放"当阻塞条件 —— 它整天在放故事，那样会永远出不了题。

性能注意
  视觉请求要 8~30 秒。出题跑在**独立线程**里，主循环照常每秒 tick，
  不然 MQTT 计数、门锁轮询都会被卡住（v0.3.x 的隐患）。

★ v0.7.0（2026-11-01）—— 治「题目重复」
  背景：10-19 ~ 11-01 的出题历史里，book 出现 24 次、chair 13 次、desk 10 次，
  最狠的是 10-28：15 道视觉题里 9 道都是 book。根因三条，逐条补：
  ① 认不出人（who=None）时**根本没喂词表** → 提示词变成"凭画面自由出题"，
     模型盯着客厅只会吐 book/chair/desk。现在 who 为空一律回落
     「二年级 + 六年级词表并集」（_any_pool），出的词必须是课本词。
  ② 词级去重只剔「喂给模型的候选池」，模型自己看着画面报 book 照样过。
     现在把最近考过的词做成**禁用清单写进提示词**，拿到结果再本地校验一遍，
     重复就让下一个模型重要一次，还重复就放弃视觉、回落词表（那边必是新词）。
  ③ 去重剔空后直接放宽到"全部词"= 纯随机。现在先退到「近 dedup_fallback_days 天」。
  另外：历史里 who 为空的那批记录，以前按人查重时被整批跳过（10-28 的 9 次 book
  全是 who="" 出的），现在 who 空 = 所有人都算出过。
  出题比例也改成画面优先：vision 90% / 词表 10% / 题库仅兜底。
  词表题补一句**带时态的例句**（报答案时念），六年级还要求孩子跟着造一句。

★ v0.7.1（2026-11-01）—— 报答案不再「一律夸」
  v0.7.0 上线后实测发现：孩子根本没答（unknown），小爱照样念「答对啦，真棒！」，
  等于白送鼓励、还会误导家长以为答对了。现在 _watch_answer 把判定结果
  （对 / 错 / 没答）登记到 _last_judged，_reveal 按结论选前缀：
  对 → PRAISE，错 → ENCOURAGE（不打击），没答 → NO_ANSWER（中性公布）。
  远程测试出题、答案监视未开启时没有记录 → 走中性说法，不假装答对。

★ v0.4.1 新增：MQTT 远程状态 / 指令通道（不用登 DSM 也能运维）
  · 订阅 kaoguan/state（retained，每 30 秒刷新一次）→ 版本、今日出题数、看画面次数、
    此刻客厅人数、累计达标秒数、今天谁回过家、上次出题时间…一条 retained 消息全都有。
  · 订阅 kaoguan/cmd 接收指令：
        restart      退出进程，交给 compose 的 restart: unless-stopped 拉起
                     （★ 换过 app 目录后靠这个，Docker 会重新解析 bind mount，
                       以后升级代码不用再点 DSM 的 停止→启动）
        status       立刻重发一份状态
        test-vision  立刻走一次看画面出题（会真的念出来）
        test-word    立刻从年级词表出一题
        test-ask     立刻按比例出一题
        speak:你好    直接让音箱说一句
    用法：mosquitto_pub -h 192.168.1.100 -p 11883 -t kaoguan/cmd -m status

课文词表（拼写题 / 视觉出题的提示词）
  放 words/ 目录，文件名用**年级名**（六年级.txt / 二年级.txt），跟 config 的
  kid_grade 对齐。格式：一行一个词，英文在前中文在后，Tab/逗号/空格分隔都行。
  改完**不用重启容器**（每次出题都重新读盘）。

用法
  python kaoguan.py                       # 正常运行
  python kaoguan.py --dry-run             # 只判定不播报（调参）
  python kaoguan.py --simulate 2          # 假装客厅有 2 个人
  python kaoguan.py --test-ask            # 立刻出一次题（自动三分支）
  python kaoguan.py --test-word           # 立刻从年级词表出一题
  python kaoguan.py --test-vision         # 立刻走一次"看画面出题"，打印取帧+模型返回
  python kaoguan.py --list-words          # 看读到哪些词表、各多少个词
  python kaoguan.py --test-speak "你好"    # 只说一句，验证音箱通道
"""
import argparse
import base64
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
from datetime import datetime, timezone, timedelta

import yaml
import paho.mqtt.client as mqtt

BASE = os.path.dirname(os.path.abspath(__file__))
VERSION = "v0.7.1"
MQTT_STATE_TOPIC = "kaoguan/state"      # 保留消息：版本/计数/此刻人数，随时可取
MQTT_CMD_TOPIC = "kaoguan/cmd"          # 远程指令：restart / status / test-* / speak:xxx

# ----------------------------------------------------------------------------
# 配置
# ----------------------------------------------------------------------------
DEFAULT_CFG = {
    "ha": {
        "url": "http://192.168.1.100:8123",
        "token": "",
        "tts_entity": "text.xiaomi_l05c_XXXX_play_text",
        "lock_key_id_entity": "sensor.lumi_wbmcn1_XXXX_lock_key_id",
        "lock_action_entity": "sensor.lumi_wbmcn1_XXXX_lock_action",
    },
    "mqtt": {"host": "192.168.1.100", "port": 11883, "username": "", "password": ""},
    "frigate": {"camera": "c300", "zone": "zone_center", "label": "person",
                "count_mode": "max"},
    # 摄像头取帧（视觉出题用）。go2rtc 无需鉴权，可用 width/height 参数缩放。
    "camera": {"snapshot_url": "", "timeout": 15},
    # 大模型（FreeLLMAPI 统一网关，OpenAI 兼容）
    "llm": {
        "enabled": False,
        "base_url": "",
        "api_key": "",
        "models": [],
        "timeout": 60,
        "max_tokens": 400,
        "temperature": 0.9,
        "word_hint_count": 60,
    },
    # 出题三路比例（%），剩下归 questions.yaml
    # ★ v0.7.0（2026-11-01 兴哥定）：改成「画面优先」——90% 看图出题，
    #   10% 纯年级词表，题库只在两条路都拿不到素材时才兜底。
    "quiz": {"vision_ratio": 90, "words_ratio": 10},
    # ★ v0.7.0：去看最近的重复率（10-28 一天 9 次 book、15 次 vision 里 9 次 book），
    #   根因有三条，这里逐个补：
    #   ① 认不出人（who=None）时根本没喂词表，模型对着客厅自由发挥 → 只会看到书/椅子/桌子；
    #   ② 视觉出的词不做本地查重，去重只作用在「喂给模型的候选池」上；
    #   ③ 剔除干净后直接放宽到「全部词」，等于回到纯随机。
    #   下面 dedup_fallback_days 就是给 ③ 用的：先退到近 2 天，还空才全放宽。
    "dedup_fallback_days": 2,
    # ★ v0.7.0：单词题的例句（带时态）。报答案时念一句含该词、带时态的英文，
    #   六年级再让孩子跟着造一句。生成结果落盘缓存，同一个词只调一次模型。
    "sentence": {
        "enabled": True,
        "timeout": 25,
        "max_tokens": 200,
        "ask_kid_from_grade": 6,      # 六年级及以上，题目里加「你也用它说一句」
        "cache_file": os.path.join(BASE, "sentence_cache.json"),
    },
    # 与门锁自动化的并行避让
    "lock_parallel": {"arrival_quiet_minutes": 0, "quiet_on_any_unlock": True},
    "kids": {},
    "kid_grade": {},
    "kid_group": {},
    "group_style": {"趣味": "plain"},
    "trigger": {
        "min_persons": 2,
        "hold_seconds": 120,
        "hold_grace_seconds": 20,
        "require_kids_home": 0,
        "random_delay_max": 90,
        "answer_delay_seconds": 15,
    },
    "window": {"after": "15:30:00", "before": "20:30:00", "mute": []},
    "limits": {"max_per_day": 3, "min_gap_minutes": 30},
    "state_file": os.path.join(BASE, "state.json"),
    "questions_file": os.path.join(BASE, "questions.yaml"),
    "words_dir": os.path.join(BASE, "words"),
    "words_alias": {},
    # ★ 2026-10-19：出题历史 + 词级去重。
    #   实测 10-18 一天出了 22 题，可见的 13 题里 book 出现 3 次、desk 2 次、read 2 次，
    #   今天又把 book / read 重新出了一遍 —— 原因是「没有任何出题记录」，纯随机抽。
    #   这里记每道题出给谁、什么词，选词时把最近 N 天出过的剔掉。
    #   dedup_days 按年级配：二年级词表 93 个（面窄，3 天足够循环）；
    #   六年级 118 个（4 天左右轮完），给 7 天留足间隔。
    "quiz_history": os.path.join(BASE, "quiz_history.jsonl"),
    "dedup_days": {"二年级": 3, "六年级": 7, "default": 5},
    # ★ 2026-10-19：对错信号 + 间隔重复。
    #   小爱不给麦克风流，但它的「对话内容」一直在 HA 里（sensor.xiaomi_l05c_XXXX_conversation），
    #   孩子对着音箱说的每个字都能拿到 —— 不需要刷固件。答案本来就是我们自己出的，
    #   拿到转写直接比对即可：对 → 间隔翻倍；错 → 明天重来；没听清 → unknown，不记账。
    "mastery_file": os.path.join(BASE, "word_mastery.json"),
    "answer_watch": {
        "enabled": True,
        "entity": "sensor.xiaomi_l05c_XXXX_conversation",
        "poll_seconds": 2,
    },
}

log = logging.getLogger("kaoguan")

REJECT_COOLDOWN = 30        # 被闸门拦下后的静默秒数（避免每秒重判刷日志）
FACE_FRESH = 900            # 人脸识别结果有效期：这么久内认出来 = "此刻就在客厅"


def deep_merge(a, b):
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_cfg(path):
    cfg = dict(DEFAULT_CFG)
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            cfg = deep_merge(cfg, yaml.safe_load(f) or {})
    return cfg


# ----------------------------------------------------------------------------
# HA 客户端
# ----------------------------------------------------------------------------
def _age_seconds(iso):
    """HA 的 ISO 时间戳（UTC）→ 距现在多少秒；解析不了返回 None。

    ★ v0.4.6 新增。用来判断"这条米家消息是不是刚发生的"——米家消息实体
      会被摄像头消息反复覆盖，只有"新鲜"的那条才算真实事件。
    """
    if not iso:
        return None
    try:
        # kaoguan.py 里是 from datetime import datetime，所以这里写 datetime.strptime
        # （不是 datetime.datetime.strptime）
        t = datetime.strptime(str(iso)[:19], "%Y-%m-%dT%H:%M:%S")
        t = t.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds()
    except Exception:
        return None


class HAClient:
    def __init__(self, cfg, dry=False):
        self.url = cfg["url"].rstrip("/")
        self.token = cfg["token"]
        self.tts = cfg["tts_entity"]
        self.lock_key_id = cfg["lock_key_id_entity"]
        self.lock_action = cfg.get("lock_action_entity")
        # ★ v0.4.6：米家消息实体（真正的实时源，见 poll_lock 里的说明）
        self.mi_msg = cfg.get("mi_message_entity")
        self.dry = dry
        self.ctx = ssl.create_default_context()
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE

    def _req(self, path, method="GET", data=None, timeout=12):
        h = {"Authorization": "Bearer " + self.token} if self.token else {}
        body = None
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            h["Content-Type"] = "application/json"
        req = urllib.request.Request(self.url + path, data=body, headers=h, method=method)
        return urllib.request.urlopen(req, timeout=timeout, context=self.ctx).read()

    def states(self):
        return json.loads(self._req("/api/states").decode("utf-8", "replace"))

    def call_service(self, domain, service, data):
        if self.dry:
            log.info("[DRY] service %s.%s %s", domain, service,
                     json.dumps(data, ensure_ascii=False))
            return True
        try:
            self._req("/api/services/%s/%s" % (domain, service), "POST", data)
            return True
        except urllib.error.HTTPError as e:
            log.error("HA 服务调用失败 %s.%s -> %s %s", domain, service, e.code,
                      e.read().decode("utf-8", "replace")[:200])
            return False
        except Exception as e:
            log.error("HA 服务调用异常 %s.%s -> %s", domain, service, e)
            return False

    def speak(self, text):
        log.info("🔊 播报：%s", text)
        return self.call_service("text", "set_value",
                                 {"entity_id": self.tts, "value": text})


# ----------------------------------------------------------------------------
# 主逻辑
# ----------------------------------------------------------------------------
class Kaoguan:
    def __init__(self, cfg, dry=False, simulate=0):
        self.cfg = cfg
        self.dry = dry
        self.simulate = simulate
        self.ha = HAClient(cfg["ha"], dry=dry)

        self.lock = threading.Lock()          # 保护 active / zone_count / seen_faces
        self.state_lock = threading.Lock()    # 保护 state.json 写入
        self.active = {}
        self.zone_count = None
        self.cam_count = None
        self.got_active_topic = False
        self.seen_faces = {}

        self.hold_ok = 0.0
        self.last_ok_at = None
        self.tick_at = None
        self.hold_milestone = 0
        self.pending_at = None
        self.reject_until = 0
        self.last_ask = None
        self.asking = False                   # 出题线程在跑（防重入）

        self.today = datetime.now().strftime("%Y-%m-%d")
        self.today_count = 0
        self.today_vision = 0
        self.arrived = {}
        self.last_key_id = None
        self.last_lock_action = None
        self.last_door_ts = None              # 最近一次"门外开锁"的时刻
        self.last_mi_msg = None               # ★v0.4.6 上一次看到的米家消息文本
        self.asked_names = []
        # ★v0.4.8：点名历史（最近几次点了谁），用于「同一人连续被点 2 次就换人」
        self.recent_picks = []

        self.questions = self._load_questions()
        self._load_state()
        # ★ 2026-10-19：词掌握度（间隔重复的依据），与 state.json 分开存
        self.mastery = {}
        self._load_mastery()

        # v0.4.1：远程状态 / 指令通道
        self.started_at = time.time()
        self._last_state_pub = 0.0
        self.mq = None

    # -- 题库 ---------------------------------------------------------------
    def _load_questions(self):
        p = self.cfg["questions_file"]
        if not os.path.exists(p):
            log.warning("题库文件不存在：%s", p)
            return {}
        with open(p, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def pick_question(self, who=None):
        """按人取一组题，随机抽一题。返回 (group, item)"""
        if not self.questions:
            return None, None
        group = self.grade_of(who) or self.cfg["kid_group"].get(who)
        if not group or group not in self.questions:
            group = random.choice(list(self.questions.keys()))
        return group, random.choice(self.questions[group])

    # -- 年级 / 词表 ---------------------------------------------------------
    def grade_of(self, who):
        """人名 → 年级（kid_grade 优先，回落 kid_group）"""
        if not who:
            return None
        g = (self.cfg.get("kid_grade") or {}).get(who)
        return g or self.cfg["kid_group"].get(who)

    def load_words(self):
        """读 words_dir 下所有 .txt，返回 {文件名(不含扩展): [(英文, 中文), ...]}

        文件格式（一行一个词，分隔符 Tab / 逗号 / 空格都行）：

            # 上海 六年级 英语单词
            [Unit 1]                ← 方括号开头当注释
            panda 熊猫
            apple,苹果
            drink water	喝水
            the Great Wall 长城      ← 词组按「最后一个空白」切开
            1. month 月              ← 行首序号自动去掉

        ★ 每次出题都重新读盘 → 改完词表**不用重启容器**。
        """
        d = self.cfg["words_dir"]
        out = {}
        if not os.path.isdir(d):
            return out
        for fn in sorted(os.listdir(d)):
            if not fn.lower().endswith(".txt"):
                continue
            items = []
            try:
                with open(os.path.join(d, fn), encoding="utf-8-sig") as f:
                    for line in f:
                        s = line.strip()
                        if not s or s[0] in "#[":
                            continue
                        s = re.sub(r"^\d+\s*[.、)）]\s*", "", s)
                        s = re.sub(r"^[-*·•]\s*", "", s)
                        if not s:
                            continue
                        parts = re.split(r"[\t,，;；]", s, maxsplit=1)
                        if len(parts) == 1:
                            hits = list(re.finditer(r"\s+", s))
                            if hits:
                                i = hits[-1].start()
                                parts = [s[:i], s[i:]]
                        en = parts[0].strip()
                        zh = parts[1].strip() if len(parts) > 1 else ""
                        if not re.search(r"[A-Za-z]", en):
                            continue
                        items.append((en, zh))
            except Exception as e:
                log.warning("词表 %s 读取失败：%s", fn, e)
                continue
            if items:
                out[os.path.splitext(fn)[0]] = items
        return out

    @staticmethod
    def _word_hint(word):
        """拼写提示：首字母 + 长度（如 'p 开头，5 个字母'）"""
        w = (word or "").strip()
        if not w:
            return ""
        if " " in w:
            return "%s 开头，%d 个单词" % (w[0].lower(), len(w.split()))
        return "%s 开头，%d 个字母" % (w[0].lower(), len(re.sub(r"[^A-Za-z]", "", w)))

    def _word_pool(self, words, who):
        """给某个人找词表。找不到返回 None。

        找的名字依次是：人名 → words_alias[人名] → **年级名** → kid_group 组名。
        先精确匹配文件名；都不中再退一步，找文件名里**含**这些名字的。
        """
        if not who:
            return None
        names = [who]
        al = self.cfg.get("words_alias") or {}
        v = al.get(who)
        if isinstance(v, str):
            names.append(v)
        elif v:
            names += [str(x) for x in v]
        for n in (self.grade_of(who), self.cfg["kid_group"].get(who)):
            if n:
                names.append(n)

        for n in names:                         # 1) 文件名精确等于某个名字
            if n in words:
                return words[n]
        for n in names:                         # 2) 文件名里含某个名字
            if not n:
                continue
            for k in sorted(words):
                if n in k:
                    return words[k]
        return None

    # ★ v0.7.0：认不出人时的词表兜底。
    #   门锁没人开锁、人脸又长期失效时，present_kids() 返回空 → who=None，
    #   旧逻辑在 pick_vision_question 里就是 `pool=[]` → 提示词变成
    #   「这次没有对应词表，请凭画面自由出题」→ 模型对着客厅只会吐
    #   book / chair / desk / computer（10-28 一天 9 次 book 就是这么来的）。
    #   现在一律回落到「二年级 + 六年级词表并集」，保证出的词是课本词。
    def _grades(self):
        """配置里出现过的所有年级（kid_grade 的值），去重、保序。"""
        out = []
        for _n, g in (self.cfg.get("kid_grade") or {}).items():
            if g and g not in out:
                out.append(g)
        return out

    def _any_pool(self, words):
        """所有年级词表合并成一个池（按 kid_grade 的年级优先，其余词表补在后面）。"""
        if not words:
            return None
        pool, seen = [], set()
        order = [g for g in self._grades() if g in words]
        order += [k for k in sorted(words) if k not in order]
        for g in order:
            for en, zh in (words.get(g) or []):
                k = (en or "").strip().lower()
                if not k or k in seen:
                    continue
                seen.add(k)
                pool.append((en, zh))
        return pool or None

    # -- 出题历史 / 词级去重（v0.5.0，2026-10-19）--------------------------
    # 为什么要有：这里以前只有「今日出了几次」的计数，没有任何「出过什么词」的记录。
    # 实测 10-18 出 22 题，可见的 13 题里 book 3 次、desk 2 次、read 2 次；
    # 10-19 又把 book / read 重出一遍 —— 二年级词表 93 个词，纯随机抽必然反复撞。
    def _dedup_days(self, who):
        m = self.cfg.get("dedup_days") or {}
        g = self.grade_of(who)
        v = m.get(g) if g else None
        if v is None:
            v = m.get("default", 5)
        try:
            return int(v)
        except Exception:
            return 5

    def recent_words(self, who, days=None, limit=None):
        """最近 N 天出给这个人的词（小写 set）。没记录返回空集。

        ★ v0.7.0 修：`who` 为空的历史记录（认不出人时出的题）**也计入**。
          原来写的是 `if who and d.get("who") != who: continue`，
          于是「妹妹」去重时会把 who="" 的十几条全跳过 ——
          10-28 那天 9 次 book 全是 who="" 出的，第二天照样再出。
          现在只要历史里没记人，就当所有人都出过，宁可多剔不可重出。
        `limit` 给了就只保留最近 limit 个（按时间倒序取最新）。返回 set。
        """
        days = self._dedup_days(who) if days is None else days
        p = self.cfg.get("quiz_history") or ""
        if not p or not os.path.exists(p):
            return set()
        cutoff = time.time() - days * 86400
        hits = []
        try:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    ts = float(d.get("ts") or 0)
                    if ts and ts < cutoff:
                        continue
                    hw = d.get("who") or ""
                    if who and hw and hw != who:
                        continue
                    w = (d.get("word") or "").strip().lower()
                    if w:
                        hits.append((ts, w))
        except Exception as e:
            log.warning("读出题历史失败：%s", e)
        if limit and len(hits) > limit:
            hits.sort(key=lambda x: x[0])
            hits = hits[-limit:]
        return {w for _, w in hits}

    def record_quiz(self, who, kind, word, zh="", model=""):
        """出一道题记一行。失败不影响出题本身。"""
        p = self.cfg.get("quiz_history") or ""
        if not p:
            return
        rec = {"ts": time.time(),
               "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               "who": who or "", "kind": kind,
               "word": word or "", "zh": zh or "", "model": model or ""}
        try:
            with open(p, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as e:
            log.warning("写出题历史失败：%s", e)

    def _dedup_filter(self, pool, who):
        """剔掉最近出过的词；剔空了退一档再剔，仍空才原样返回。"""
        if not pool:
            return pool
        days = self._dedup_days(who)
        recent = self.recent_words(who, days)
        if not recent:
            return pool
        kept = [(en, zh) for en, zh in pool
                if (en or "").strip().lower() not in recent
                or self._is_due(who, en)]
        if kept:
            log.info("词级去重：%s 最近 %d 天出过 %d 词 → 候选 %d/%d",
                     who or "随机", days, len(recent), len(kept), len(pool))
            return kept
        # ★ v0.7.0：剔空了先退到「近 N 天」，别一步放宽到全部。
        #   词表本来就只有 90~120 个词、一天出 20 题，7 天窗口必然剔光，
        #   直接放宽等于回到纯随机 —— 这就是 book/book/book 的来源之一。
        fb = int(self.cfg.get("dedup_fallback_days", 2) or 2)
        if fb and fb < days:
            recent2 = self.recent_words(who, fb)
            kept2 = [(en, zh) for en, zh in pool
                     if (en or "").strip().lower() not in recent2
                     or self._is_due(who, en)]
            if kept2:
                log.info("词级去重：%s 近 %d 天剔空，退到近 %d 天 → 候选 %d/%d",
                         who or "随机", days, fb, len(kept2), len(pool))
                return kept2
        log.info("词级去重：%s 的 %d 个词近 %d 天都出过了，本轮放宽",
                 who or "随机", len(pool), days)
        return pool

    # -- 掌握度 / 间隔重复（v0.6.0，2026-10-19）----------------------------
    # 数据长这样（word_mastery.json）：
    #   {"妹妹": {"cat": {"seen":3,"right":2,"wrong":1,
    #                       "interval":2,"next":1792...,"last":1792...}}}
    # interval 单位=天；next 是下次该复习的 epoch。
    def _load_mastery(self):
        p = self.cfg.get("mastery_file")
        try:
            if p and os.path.exists(p):
                d = json.load(open(p, encoding="utf-8"))
                if isinstance(d, dict):
                    self.mastery = d
                    log.info("掌握度已加载：%d 人 / %d 词", len(d),
                             sum(len(v or {}) for v in d.values()))
        except Exception as e:
            log.warning("加载掌握度失败（按空处理）：%s", e)
            self.mastery = {}

    def _save_mastery(self):
        p = self.cfg.get("mastery_file")
        if not p:
            return
        try:
            with self.state_lock:
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(self.mastery, f, ensure_ascii=False, indent=1)
        except Exception as e:
            log.warning("保存掌握度失败：%s", e)

    def _is_due(self, who, word):
        """这个词是不是「到期该复习了」。

        注意 next==0 表示**还没被判过对错**（unknown），这时返回 False，
        交给词级去重去管 —— 否则同一个词会当天反复出。
        """
        it = (self.mastery.get(who or "") or {}).get((word or "").strip().lower())
        if not it or not it.get("next"):
            return False
        return float(it["next"]) <= time.time()

    def update_mastery(self, who, word, ok):
        w = (word or "").strip().lower()
        if not w:
            return
        m = self.mastery.setdefault(who or "", {})
        it = m.get(w) or {"seen": 0, "right": 0, "wrong": 0,
                          "interval": 0, "next": 0, "last": 0}
        it["seen"] = int(it.get("seen", 0)) + 1
        if ok:
            it["right"] = int(it.get("right", 0)) + 1
            cur = int(it.get("interval", 0) or 0)
            it["interval"] = 1 if cur < 1 else min(cur * 2, 30)
        else:
            it["wrong"] = int(it.get("wrong", 0)) + 1
            it["interval"] = 1
        now = time.time()
        it["last"] = now
        it["next"] = now + int(it["interval"]) * 86400
        m[w] = it
        self._save_mastery()
        log.info("📊 掌握度 %s/%s：%s（间隔 %d 天，下次 %s）",
                 who or "?", word, "✔ 对" if ok else "✘ 错", it["interval"],
                 datetime.fromtimestamp(it["next"]).strftime("%m-%d %H:%M"))

    def _due_first(self, pool, who):
        """到期要复习的词优先出；没有到期的就用原池。"""
        due = [p for p in pool if self._is_due(who, p[0])]
        if due:
            log.info("📌 复习优先：%s 有 %d 个到期词（%s）", who or "随机", len(due),
                     "、".join(e for e, _ in due[:6]))
            return due
        return pool

    # -- 答案监听（v0.6.0）--------------------------------------------------
    # 小爱不给麦克风流，但「对话内容」一直在 HA：sensor.xiaomi_l05c_XXXX_conversation。
    # 孩子对着音箱说的每个字都能拿到（history 里还有前两轮），所以**不用刷固件**。
    @staticmethod
    def _norm_text(t):
        t = (t or "").lower().strip()
        t = re.sub(r"[^a-z0-9\u4e00-\u9fff ]+", " ", t)
        return re.sub(r"\s+", " ", t).strip()

    @classmethod
    def _judge_answer(cls, text, word, zh):
        """True=答对 False=答错 None=没听清/不是回答（不记账）"""
        t = cls._norm_text(text)
        if not t:
            return None
        w = cls._norm_text(word)
        z = cls._norm_text(zh)
        # 英文答案按词边界匹配（"这是 cat" 也算），多词短语允许空格差异
        if w:
            if re.search(r"\b" + re.escape(w) + r"\b", t):
                return True
            if re.sub(r"\s+", "", w) in re.sub(r"\s+", "", t):
                return True
        if z and z in t:
            return True
        # 明显是别的指令（孩子顺手让小爱干活）→ 不是回答
        if any(k in t for k in ("播放", "歌单", "音量", "声音", "关闭", "静音", "小爱")):
            return None
        # 整句像英文/单词 → 认作「答了但不对」；其余中文闲聊 → 不记账
        if re.fullmatch(r"[a-z0-9 ]{1,30}", t):
            return False
        return None

    def _watch_answer(self, who, word, zh, start_ts, end_ts):
        """盯小爱对话实体，等孩子把答案说出来。到点没听到就放弃（不计错）。"""
        # ★ v0.7.1：开局先登记「这题还没判定」，_reveal 靠它决定念不念「答对啦」。
        #   否则没喂答案的孩子也被夸一顿（2026-11-01 实测：妹妹没答，
        #   小爱照样念「答对啦，真棒！」，等于白送鼓励，还会误导家长）。
        self._last_judged = {"word": (word or "").strip().lower(), "who": who,
                             "ok": None, "ts": time.time()}
        cfg = self.cfg.get("answer_watch") or {}
        if not cfg.get("enabled"):
            return
        ent = cfg.get("entity") or "sensor.xiaomi_l05c_XXXX_conversation"
        poll = float(cfg.get("poll_seconds") or 2)
        seen = None
        time.sleep(max(0.0, start_ts - time.time()))
        while time.time() < end_ts:
            try:
                raw = self.ha._req("/api/states/" + ent, timeout=6)
                d = json.loads(raw.decode("utf-8", "replace"))
                content = ((d.get("attributes") or {}).get("content")
                           or d.get("state") or "").strip()
                changed = d.get("last_changed") or ""
            except Exception as e:
                log.debug("读对话实体失败：%s", e)
                time.sleep(poll)
                continue
            # 只认「出题之后」才变化的内容，避免把上一句当成这次回答
            fresh = False
            try:
                fresh = datetime.fromisoformat(
                    changed.replace("Z", "+00:00")).timestamp() >= start_ts - 5
            except Exception:
                fresh = True
            if content and fresh and content != seen:
                seen = content
                ok = self._judge_answer(content, word, zh)
                if ok is not None:
                    log.info("🎤 听到回答「%s」→ %s", content, "对" if ok else "错")
                    self.update_mastery(who, word, ok)
                    self._last_judged = {"word": (word or "").strip().lower(),
                                         "who": who, "ok": bool(ok),
                                         "ts": time.time(), "said": content}
                    return
                log.info("🎤 听到「%s」但判不出对错，继续等", content)
            time.sleep(poll)
        log.info("⌛ %s 的「%s」没等到回答（unknown，不记账）", who or "?", word)
        self._last_judged = {"word": (word or "").strip().lower(), "who": who,
                             "ok": None, "ts": time.time()}

    def pick_word(self, who=None):
        """按人取一份词表、随机抽一个词。返回 dict；None = 一个词都没有"""
        words = self.load_words()
        if not words:
            return None
        # ★ v0.7.0：who 为空时不再「随便挑一份词表」（原来 random.choice 可能挑到
        #   任意 txt），统一用「该生年级 → 两套词表并集」的顺序，保证是 2 年级/6 年级的词。
        pool = (self._word_pool(words, who) if who else None) or self._any_pool(words)
        if not pool:
            return None
        pool = self._dedup_filter(pool, who)
        pool = self._due_first(pool, who)
        en, zh = random.choice(pool)
        grade = self.grade_of(who)
        # ★ v0.7.0：词表题也配一句带时态的例句（报答案时念）。
        sent, sent_zh = self._sentence_for(en, zh, grade)
        return {"word": en, "zh": zh or en, "hint": self._word_hint(en),
                "sentence": sent, "sentence_zh": sent_zh, "grade": grade}

    # -- 视觉出题（v0.4.0 核心）---------------------------------------------
    @property
    def ctx(self):
        return self.ha.ctx

    def grab_frame(self):
        """抓一张摄像头快照（JPEG bytes）。失败返回 None（不抛异常）。"""
        url = (self.cfg.get("camera") or {}).get("snapshot_url")
        if not url:
            return None
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "kaoguan/%s" % VERSION})
            return urllib.request.urlopen(
                req, timeout=(self.cfg["camera"].get("timeout") or 15),
                context=self.ctx).read()
        except Exception as e:
            log.warning("取帧失败（%s）：%s", url, e)
            return None

    def _llm_chat(self, messages, model, max_tokens=None, timeout=None):
        """调 FreeLLMAPI 的 /v1/chat/completions，返回 (content, routed_via)。"""
        llm = self.cfg["llm"]
        body = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens or llm.get("max_tokens", 400),
            "temperature": llm.get("temperature", 0.9),
        }
        req = urllib.request.Request(
            llm["base_url"].rstrip("/") + "/chat/completions",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + llm.get("api_key", "")},
            method="POST")
        resp = urllib.request.urlopen(
            req, timeout=timeout or llm.get("timeout", 60), context=self.ctx)
        data = json.loads(resp.read().decode("utf-8", "replace"))
        content = ((data.get("choices") or [{}])[0].get("message", {}) or {}).get("content", "")
        return content, resp.headers.get("X-Routed-Via")

    # 中文年级 → 数字。用于提示词里的难度分档（六年级=6 走"别出启蒙词"，
    # 二年级=2 走"别出超纲词"，两边的护栏方向正好相反，所以必须分档）。
    _CN_NUM = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
               "六": 6, "七": 7, "八": 8, "九": 9}

    @classmethod
    def _grade_num(cls, grade):
        m = re.search(r"[一二三四五六七八九]", str(grade or ""))
        return cls._CN_NUM.get(m.group(0), 0) if m else 0

    # -- 例句（v0.7.0）-------------------------------------------------------
    # 兴哥 2026-11-01 的原话：「最好加一句根据单词产生的事态的句子」。
    # 目标：报答案的时候不只念一个词，还念一句**含这个词、时态完整**的英文短句，
    # 让孩子听到「词在句子里长什么样」；六年级再让他跟着造一句。
    # 视觉题原本就有 sentence 字段，词表题一直是裸词，这里补齐。
    @classmethod
    def _tense_hint(cls, grade):
        gnum = cls._grade_num(grade)
        if gnum >= 6:
            return ("句子要用小学六年级学过的一般现在时或一般过去时，"
                    "时态必须正确（比如 I read a book yesterday. / She reads books every day.）")
        if gnum:
            return "句子只用一般现在时，主谓要完整（比如 This is my book. / I like books.）"
        return "句子尽量简单，一般现在时就行。"

    def _sentence_cache_path(self):
        return ((self.cfg.get("sentence") or {}).get("cache_file") or "").strip()

    def _load_sentence_cache(self):
        p = self._sentence_cache_path()
        if not p:
            return {}
        try:
            if os.path.exists(p):
                d = json.load(open(p, encoding="utf-8"))
                if isinstance(d, dict):
                    return d
        except Exception as e:
            log.warning("读例句缓存失败（按空处理）：%s", e)
        return {}

    def _save_sentence_cache(self, cache):
        p = self._sentence_cache_path()
        if not p:
            return
        try:
            with self.state_lock:
                tmp = p + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(cache, f, ensure_ascii=False, indent=1)
                os.replace(tmp, p)
        except Exception as e:
            log.warning("写例句缓存失败：%s", e)

    @staticmethod
    def _parse_sentence_json(text):
        """从模型输出里抠 {sentence, sentence_zh}。容忍围栏、废话、截断。"""
        if not text:
            return None
        t = re.sub(r"```[a-zA-Z]*", "", text).strip()
        for m in re.finditer(r"\{", t):
            depth, end = 0, -1
            for i in range(m.start(), len(t)):
                if t[i] == "{":
                    depth += 1
                elif t[i] == "}":
                    depth -= 1
                    if depth == 0:
                        end = i
                        break
            raw = t[m.start():end + 1] if end > 0 else t[m.start():].rstrip().rstrip(",") + "}"
            try:
                d = json.loads(raw)
            except Exception:
                continue
            if isinstance(d, dict) and str(d.get("sentence") or "").strip():
                return d
        # 兜底：裸文本里找第一行英文句子
        for ln in t.splitlines():
            s = ln.strip().strip('"“”「」')
            # 去掉 "sentence:" / "sentence:" 这类键名残留
            s = re.sub(r"^[A-Za-z_]{2,14}\s*[:：=]\s*", "", s)
            s = s.strip().strip('"“”「」')
            if 3 <= len(s) <= 140 and re.search(r"[A-Za-z]{2,}\s+[A-Za-z]{2,}", s) \
                    and re.search(r"[.!?]", s):
                return {"sentence": s}
        return None

    def _sentence_for(self, word, zh, grade):
        """给单词配例句，返回 (英文句, 中文句)。拿不到就返回 ("", "") —— 不阻塞出题。"""
        sc = self.cfg.get("sentence") or {}
        if not sc.get("enabled", True):
            return "", ""
        w = (word or "").strip()
        if not w or not re.search(r"[A-Za-z]", w):
            return "", ""
        key = w.lower()
        cache = self._load_sentence_cache()
        hit = cache.get(key)
        if isinstance(hit, dict):
            if hit.get("s"):
                log.info("例句命中缓存：%s → %s", w, hit.get("s"))
                return hit.get("s", ""), hit.get("z", "")
            # 负缓存：这个词上次生成失败（模型吐垃圾/不含目标词），7 天内别再白等一次
            if hit.get("fail") and time.time() - float(hit["fail"]) < 7 * 86400:
                log.info("例句上次生成失败（%s），7 天内不再重试", w)
                return "", ""

        llm = self.cfg.get("llm") or {}
        if not (llm.get("enabled") and llm.get("models")):
            return "", ""
        prompt = (
            "你在给中国小学生准备英语例句。单词：%s（%s）。\n"
            "用这个单词造一句地道、简短的英文例句。%s\n"
            "再给出这句的中文翻译。\n"
            '只输出这个 JSON，字段名不许改：{"sentence":"英文例句","sentence_zh":"中文翻译"}\n'
            "⚠️ 第一个字符必须是 { ，最后一个必须是 } ，不要解释、不要 markdown。"
        ) % (w, zh or "无", self._tense_hint(grade))
        messages = [{"role": "user", "content": prompt}]
        for model in list(llm["models"])[:2]:
            try:
                content, _via = self._llm_chat(
                    messages, model,
                    max_tokens=int(sc.get("max_tokens", 200) or 200),
                    timeout=float(sc.get("timeout", 25) or 25))
            except Exception as e:
                log.warning("例句模型 %s 失败：%s", model, e)
                continue
            d = self._parse_sentence_json(content)
            if not d:
                continue
            s = str(d.get("sentence") or "").strip().strip("。.").strip()
            z = str(d.get("sentence_zh") or d.get("zh") or "").strip()
            if not s or not (4 <= len(s) <= 160):
                continue
            if re.search(r"[\u4e00-\u9fff]", s):
                log.warning("例句混进中文（%s），丢弃", s)
                continue
            if len(s.split()) < 3:
                log.warning("例句太短（%s），丢弃", s)
                continue
            if w.lower() not in s.lower():
                # 模型没用上这个词 → 不算合格，别念给孩子听
                log.warning("例句不含目标词（%s / %s），丢弃", w, s)
                continue
            cache[key] = {"s": s, "z": z, "ts": int(time.time())}
            self._save_sentence_cache(cache)
            log.info("例句已生成：%s → %s", w, s)
            return s, z
        cache[key] = {"fail": int(time.time())}
        self._save_sentence_cache(cache)
        log.warning("例句这次没拿到（%s），先记失败，7 天后再试", w)
        return "", ""

    def _vision_prompt(self, who, grade, words_hint, banned=""):
        subject = ("画面里的孩子是「%s」" % who) if who else "画面里的孩子"
        grade_txt = ("（%s学生）" % grade) if grade else ""
        # ★ v0.4.2 加难度护栏：
        #   实测给六年级的姐姐出题时，模型看着茶几答了 "table"（桌子）——
        #   词表里没这个词就自由发挥，而自由发挥**没有难度下限**，于是给了个
        #   启蒙级基础词，等于白考。低年级反过来怕"超纲"，所以按年级分档给护栏。
        #   ⚠️ 措辞要短、不能举例刺激模型"讲道理"：第一版写了"⚠️/例如 dog chair"，
        #      结果 agnes/dots3 直接开始一大段"让我看看词表里有什么…"的推理，
        #      JSON 被顶掉、四个模型全挂。现在改成一句话，另加禁思考指令。
        gnum = self._grade_num(grade)
        if gnum >= 4:
            level_guard = ("难度：优先用词表里的词；词表里实在没有，就用画面里最显眼物品的"
                           "常用词，别退回到一二年级的启蒙词。")
        elif gnum:
            level_guard = ("难度：优先用词表里的词；词表里实在没有，就用画面里最显眼物品的"
                           "常用词，别用超纲的难词。")
        else:
            level_guard = "难度：对得上孩子的年级，别过难也别过浅。"
        # ★ v0.7.0：把「最近考过的词」显式写进提示词。
        #   只靠本地剔候选池没用 —— 模型看画面挑词时，画面里就是书桌和书，
        #   它会自己把 book/desk 捡回来，哪怕词表里已经被剔掉了。
        ban_txt = ""
        if banned:
            ban_txt = ("⚠️ 这些词最近已经考过了，这次绝对不要再用：%s\n" % banned)
        # ★★ v0.4.2 关键改写：原来是「请完成三件事：1. … 2. … 3. …」，
        #   结果 agnes/dots3 把这串编号**当成了输出格式**，真的吐了一篇
        #   "1. **分析画面内容**：…" 的分析长文，max_tokens 还在半路就截断，
        #   JSON 根本没出现 → 4 个模型全挂、出题成功率只有 50%。
        #   改成「把下面这个 JSON 的 5 个字段填出来」——把编号从输出里彻底拿掉，
        #   字段说明写进 JSON 值的位置上，模型一次就给对了。
        return (
            "你是家里的英语小考官，正在看家庭客厅摄像头拍到的实时画面。\n"
            "%s%s\n"
            "这个年级课本上正在学的词表（英文=中文）：\n%s\n\n"
            "%s"
            "把下面 5 个字段填好，只输出一个 JSON：\n"
            "scene = 一句中文，说画面里孩子在做什么、身边有什么物品（看不清就别硬猜，不超过 25 字）\n"
            "word = 从上面词表挑一个跟画面最搭的英文单词\n"
            "zh = word 的中文意思\n"
            "sentence = 用 word 造一句英文，时态要正确（一般现在时或一般过去时）\n"
            "from_list = true\n"
            '输出格式：{"scene":"","word":"","zh":"","sentence":"","from_list":true}\n'
            "%s\n"
            "⚠️ 直接输出这个 JSON：第一个字符必须是 { ，最后一个字符必须是 } 。"
            "不要写分析、不要写步骤、不要编号、不要解释、不要 markdown 代码块。"
        ) % (subject, grade_txt, words_hint, ban_txt, level_guard)

    @staticmethod
    def _parse_vision_json(text):
        """从模型输出里抠出题目 JSON。

        模型（尤其带思维链的）常常在 JSON 前后夹一堆废话，所以：
        先做花括号配对找出所有候选对象，逐个 json.loads，取第一个带 word 的；
        对「有 { 没有 }」（输出被 max_tokens 截断）做补右括号抢救；
        都不行再用宽松正则兜底（容忍不加引号、用 = 号、带 ** 加粗的写法）。
        """
        if not text:
            return None
        t = re.sub(r"```[a-zA-Z]*", "", text).strip()
        cands = []
        for m in re.finditer(r"\{", t):
            depth = 0
            closed = False
            for i in range(m.start(), len(t)):
                if t[i] == "{":
                    depth += 1
                elif t[i] == "}":
                    depth -= 1
                    if depth == 0:
                        cands.append(t[m.start():i + 1])
                        closed = True
                        break
            if not closed:
                # ★ 被截断的半个 JSON：补一个右括号再试（v0.4.2）
                cands.append(t[m.start():].rstrip().rstrip(",") + "}")
        for c in cands:
            try:
                d = json.loads(c)
            except Exception:
                continue
            if isinstance(d, dict) and str(d.get("word") or "").strip():
                return d
        # 宽松正则兜底：容忍 "word": "x" / word: x / word=x / **word**：x
        # ★ 注意分支顺序：引号版必须放在无引号版**前面**，且前面不能先吃掉引号，
        #   否则 `word: "desk"` 会被无引号分支匹配成 `desk"`（v0.4.2 自检抓到）。
        _NOQ = r'[^\s.,，。；;:：!?！？、}\]\n"\'*`|]'

        def grab(key, maxlen=200):
            pat = (r'[*"\'`\s]*%s[*"\'`\s]*[:：=]\s*'
                   r'(?:"([^"]{1,%d})"|\'([^\']{1,%d})\'|(%s{1,%d}))'
                   % (key, maxlen, maxlen, _NOQ, maxlen))
            mm = re.search(pat, t, re.IGNORECASE)
            if not mm:
                return ""
            return (mm.group(1) or mm.group(2) or mm.group(3) or "").strip()
        w = grab("word", 40)
        if not w or not re.search(r"[A-Za-z]", w):
            return None
        return {"word": w, "scene": grab("scene"),
                "zh": grab("zh", 60), "sentence": grab("sentence")}

    @staticmethod
    def _clean_vision(d, who):
        """把模型输出规整成可用的题目；不合格返回 None。"""
        word = str(d.get("word") or "").strip().strip("。.,，")
        if not word or not re.search(r"[A-Za-z]", word) or len(word) > 40:
            return None
        scene = str(d.get("scene") or "").strip()
        # ★ v0.7.0：模型（尤其 dots-3）会把字段说明**原样抄进 scene**，
        #   实测念出来就是「画面里的孩子正在做什么、身边有什么具体看得见的物品…」。
        #   这里认出说明原文就整段丢掉；正常但太长的描述截到 40 字以内（TTS 念得完）。
        if scene:
            scene = re.sub(r"\s+", "", scene)
            if any(k in scene for k in ("只写真的看到", "画面里的孩子正在做什么",
                                        "用一句中文说出", "身边有什么具体看得见")):
                scene = ""
            elif len(scene) > 40:
                segs = [s for s in re.split(r"[，,；;。！？!?]", scene) if s]
                out = ""
                for s in segs:
                    if len(out) + len(s) > 40:
                        break
                    out += s + "，"
                scene = out.rstrip("，") or segs[0][:40]
        zh = str(d.get("zh") or "").strip() or word          # 没给中文释义就退回用英文本身
        sent = str(d.get("sentence") or "").strip()
        from_list = bool(d.get("from_list"))
        return {"word": word, "zh": zh, "scene": scene,
                "sentence": sent, "from_list": from_list,
                "hint": Kaoguan._word_hint(word), "who": who}

    def pick_vision_question(self, who=None):
        """看画面出题。返回题目 dict；失败返回 None（调用方回落词表）。"""
        llm = self.cfg.get("llm") or {}
        if not llm.get("enabled") or not llm.get("base_url") or not llm.get("models"):
            return None
        img = self.grab_frame()
        if not img:
            return None
        b64 = base64.b64encode(img).decode("ascii")

        grade = self.grade_of(who)
        n = int(llm.get("word_hint_count", 60) or 0)
        all_words = self.load_words()
        # ★ v0.7.0：认不出人（who=None）也必须喂词表。
        #   旧代码 who 为空时 _word_pool 返回 None → pool=[] → 提示词变成
        #   「这次没有对应词表，请凭画面自由出题」。客厅画面最显眼的就是书/椅子/书桌，
        #   模型于是反复吐 book、chair、desk（10-28 一天 9 次 book）。
        #   现在退回「二年级 + 六年级词表并集」，出的词至少是课本词。
        pool = ((self._word_pool(all_words, who) if who else None)
                or self._any_pool(all_words) or [])
        # ★ 2026-10-19：喂给模型的候选先剔掉最近出过的词。
        #   视觉题最容易撞画面里的固定物品（书桌→desk、书→book 昨天今天各来一遍）。
        hint_pool = self._dedup_filter(pool, who) if pool else pool
        if hint_pool and n:
            words_hint = "、".join("%s=%s" % (en, zh or en) for en, zh in hint_pool[:n])
        else:
            words_hint = "（这次没有对应词表，请凭画面自由出题）"
        # ★ from_list **本地核对**，不信模型自己报的：
        #   实测 agnes-2.5-flash 会把没喂给它的词（plate）也说成 from_list=true，
        #   照着念「这个词你课本上学过」就是假话。
        #   注意：这里要按**实际喂进去的 hint_pool** 判，不能用原始 pool，
        #   否则被去重剔掉的词模型没看见、却可能被误判成 from_list。
        pool_set = {en.strip().lower() for en, _ in (hint_pool or [])}

        # ★ v0.7.0：最近出过的词做成「禁用清单」写进提示词。
        #   剔候选池只能挡住"从列表里挑"，挡不住"模型自己看着画面报 book"，
        #   所以必须在提示词里点名禁掉，另外拿到结果后再本地校验一遍。
        recent = self.recent_words(who, limit=25)
        banned = "、".join(sorted(recent))
        if banned:
            log.info("视觉禁用词（最近考过 %d 个）：%s", len(recent), banned[:80])

        prompt = self._vision_prompt(who, grade, words_hint, banned=banned)
        messages = [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url",
                 "image_url": {"url": "data:image/jpeg;base64," + b64}},
            ],
        }]

        dup_retry = 0
        for model in llm["models"]:
            t0 = time.time()
            try:
                content, via = self._llm_chat(messages, model)
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", "replace")[:160]
                log.warning("视觉模型 %s HTTP %s：%s", model, e.code, body)
                continue
            except Exception as e:
                log.warning("视觉模型 %s 异常：%s", model, e)
                continue
            q = self._parse_vision_json(content)
            q = self._clean_vision(q, who) if q else None
            if q:
                wl = q["word"].strip().lower()
                q["from_list"] = wl in pool_set
                # ★ v0.7.0 本地查重：模型没听招呼，报了个刚考过的词 → 换下一个模型再要一次。
                #   最多重试 1 次，还重复就放弃视觉这条路（回落词表，词表那边必然是新词）。
                if wl in recent and not self._is_due(who, wl):
                    if dup_retry < 1:
                        dup_retry += 1
                        log.warning("视觉出词重复（%s 最近考过），换模型重试一次", q["word"])
                        continue
                    log.warning("视觉出词仍然重复（%s），放弃本路 → 回落词表", q["word"])
                    return None
                if not q.get("sentence"):
                    s, _z = self._sentence_for(q["word"], q.get("zh"), grade)
                    if s:
                        q["sentence"] = s
                elif q["word"].strip().lower() not in q["sentence"].lower():
                    # 模型给的例句里没有目标词 → 不可信，重新配一句
                    log.warning("视觉例句不含目标词（%s / %s），重新生成",
                                q["word"], q["sentence"][:40])
                    s, _z = self._sentence_for(q["word"], q.get("zh"), grade)
                    q["sentence"] = s or ""
                q["model"] = model
                q["via"] = via
                q["cost_s"] = round(time.time() - t0, 1)
                return q
            log.warning("视觉模型 %s 返回解析失败：%r", model, (content or "")[:160])
        return None

    # -- 出题素材三分支 -------------------------------------------------------
    def _pick_material(self, who, mode="auto"):
        """返回 (kind, payload)。kind ∈ vision / word / bank。"""
        q = self.cfg.get("quiz") or {}
        vr = int(q.get("vision_ratio", 0) or 0)
        wr = int(q.get("words_ratio", 0) or 0)

        if mode == "vision":
            plan = ["vision", "word", "bank"]
        elif mode == "word":
            plan = ["word", "bank"]
        else:
            r = random.randint(1, 100)
            if r <= vr:
                plan = ["vision", "word", "bank"]
            elif r <= vr + wr:
                plan = ["word", "bank"]
            else:
                plan = ["bank", "word"]

        for step in plan:
            if step == "vision":
                v = self.pick_vision_question(who)
                if v:
                    return "vision", v
                log.info("看画面出题没成（取帧/模型不可用），回落词表")
            elif step == "word":
                w = self.pick_word(who)
                if w:
                    return "word", w
            else:
                g, it = self.pick_question(who)
                if it:
                    return "bank", (g, it)
        return None, None

    # -- 状态持久化 ---------------------------------------------------------
    def _load_state(self):
        p = self.cfg["state_file"]
        if not os.path.exists(p):
            return
        try:
            d = json.load(open(p, encoding="utf-8"))
            # ★ v0.4.5：last_door_ts 也持久化。
            #   原来它只在内存里 —— 容器一重启（换代码、崩了被拉起）避让窗口就归零，
            #   正好撞在「孩子刚开门回家」那一刻，kaoguan 就会盖掉门锁那条英语播放，
            #   直接违反「门锁和 kaoguan 并行不冲突」。所以必须落盘。
            #   过期的值不用清：_can_ask 里算 remaining 会自然归 0。
            door = d.get("last_door_ts")
            if door:
                self.last_door_ts = float(door)
            if d.get("date") == self.today:
                self.today_count = d.get("count", 0)
                self.today_vision = d.get("vision_count", 0)
                self.last_ask = d.get("last_ask")
                self.arrived = d.get("arrived", {})
                self.asked_names = d.get("asked", [])
                # ★v0.4.8：点名历史也要落盘 —— 否则容器重启后"连二次必换"的判断
                #   归零，连着两次点同一个人的概率又回来了。
                self.recent_picks = d.get("recent_picks", []) or []
        except Exception:
            pass

    def _save_state(self):
        try:
            with self.state_lock:
                json.dump({"date": self.today, "count": self.today_count,
                           "vision_count": self.today_vision,
                           "last_ask": self.last_ask, "arrived": self.arrived,
                           "asked": self.asked_names,
                           "recent_picks": self.recent_picks,
                           "last_door_ts": self.last_door_ts},
                          open(self.cfg["state_file"], "w", encoding="utf-8"),
                          ensure_ascii=False, indent=1)
        except Exception as e:
            log.warning("状态保存失败：%s", e)

    # -- MQTT ---------------------------------------------------------------
    def start_mqtt(self):
        m = self.cfg["mqtt"]
        cid = m.get("client_id") or "kaoguan"
        try:
            self.mq = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid)
        except AttributeError:                       # 老版 paho
            self.mq = mqtt.Client(client_id=cid)
        if m.get("username"):
            self.mq.username_pw_set(m["username"], m.get("password") or "")
        self.mq.on_connect = self._on_connect
        self.mq.on_message = self._on_message
        # 连不上就重试，别让容器崩成重启循环：mosquitto 比 kaoguan 晚起来、
        # 或者中间网络抖一下，早期版本会直接退出 → restart 策略再拉起 → 死循环。
        for attempt in range(1, 121):
            try:
                self.mq.connect(m["host"], m["port"], 60)
                self.mq.loop_start()
                return
            except Exception as e:
                log.warning("MQTT 连接失败（第 %d 次）：%s；10 秒后重试", attempt, e)
                time.sleep(10)
        raise SystemExit("MQTT 一直连不上，放弃启动")

    def _topics(self):
        f = self.cfg["frigate"]
        return ("frigate/%s/%s/active" % (f["zone"], f["label"]),
                "frigate/%s/%s/active" % (f["camera"], f["label"]))

    def _on_connect(self, c, u, f, rc, props=None):
        zone_t, cam_t = self._topics()
        log.info("MQTT 已连接 %s:%s (rc=%s)", self.cfg["mqtt"]["host"],
                 self.cfg["mqtt"]["port"], rc)
        c.subscribe([("frigate/events", 0), (zone_t, 0), (cam_t, 0),
                     (MQTT_CMD_TOPIC, 0)])
        # ⚠️ `_on_connect` 里抛异常会**直接干掉 paho 的网络线程**，连接就再也回不来了
        #    （本地实测踩过：方法名写错 → AttributeError → 整个 MQTT 循环静默退出）。
        #    这里宁可丢掉一条状态消息，也不能让连接线程死掉。
        try:
            self.publish_state(force=True)
        except Exception as e:
            log.warning("上线时推状态失败（不影响连接）：%s", e)

    def _on_message(self, c, u, msg):
        zone_t, cam_t = self._topics()
        if msg.topic == MQTT_CMD_TOPIC:
            try:
                self._on_cmd(msg.payload.decode("utf-8", "replace").strip())
            except Exception as e:
                log.error("执行远程指令失败：%s", e)
            return
        if msg.topic == zone_t:
            try:
                self.zone_count = int(float(msg.payload))
                self.got_active_topic = True
            except Exception:
                pass
            return
        if msg.topic == cam_t:
            try:
                self.cam_count = int(float(msg.payload))
            except Exception:
                pass
            return
        if msg.topic != "frigate/events":
            return
        try:
            d = json.loads(msg.payload)
        except Exception:
            return
        a = d.get("after") or {}
        if a.get("camera") != self.cfg["frigate"]["camera"]:
            return
        if a.get("label") != self.cfg["frigate"]["label"]:
            return
        eid = a.get("id")
        if not eid:
            return
        typ = d.get("type")
        zones = a.get("current_zones") or a.get("zones") or []
        # ★ 2026-10-01 修正：`max` 模式下**不再要求检测框落进 zone**。
        #   原因（实测实锤）：① zone_center 是配置里写死的占位坐标，从没按实际视野校准，
        #   人一坐到餐桌边检测框中心 y≈0.89 就整框出界；② 更关键的是，这台 Frigate 实测
        #   根本不发布 `frigate/<camera>/<object>/active` 主题（旁听 25 秒只看到
        #   `.../zone/zone_center/state`），所以 zone_count / cam_count 恒为 None，
        #   人数只能靠 `frigate/events` 事件流来数 —— 事件流再按 zone 过滤就永远是 0。
        #   现在 max 模式数"这台摄像头上所有在跟踪的 person"，zone_only 才按 zone 严格算。
        zone_only = str(self.cfg["frigate"].get("count_mode", "max")).lower() == "zone_only"
        hit = zone_only is False or self.cfg["frigate"]["zone"] in zones

        sub = a.get("sub_label")
        if sub and sub in self.cfg["kids"].values() and hit:
            with self.lock:
                self.seen_faces[sub] = time.time()
            if sub not in self.arrived:
                self.arrived[sub] = datetime.now().strftime("%H:%M:%S")
                log.info("🙂 人脸认出 %s，今天也算回过家了", sub)
                self._save_state()
        with self.lock:
            if typ == "end":
                self.active.pop(eid, None)
            elif hit:
                self.active.setdefault(eid, time.time())
            else:
                self.active.pop(eid, None)

    # -- 远程状态 / 指令通道（v0.4.1）---------------------------------------
    # 为什么要有这个：容器日志在 DSM 里要登录点半天、还常常不刷新；换代码又要
    # stop→换目录→start 手动点。挂一条 MQTT 通道后，状态随时可读、重启一条命令，
    # 以后升级不用再碰 DSM 界面。任何能连 mosquitto 的机器都能用。
    def publish_state(self, force=False):
        if self.mq is None:
            return
        now = time.time()
        if not force and now - self._last_state_pub < 30:
            return
        self._last_state_pub = now
        # ★ v0.4.4：把「门锁避让还剩多久」也报出来。门锁和 kaoguan 是**并行**关系，
        #   靠的不是共享状态而是「门口开锁后 kaoguan 自己安静 N 分钟」。
        #   不加这个字段就没法从外面判断避让到底有没有生效 —— 只能干等。
        _q = float((self.cfg.get("lock_parallel") or {}).get("arrival_quiet_minutes", 0) or 0)
        _left = 0.0
        if _q > 0 and self.last_door_ts:
            _left = max(0.0, _q * 60 - (now - self.last_door_ts))
        payload = {
            "version": VERSION,
            "started_at": datetime.fromtimestamp(self.started_at).strftime("%Y-%m-%d %H:%M:%S"),
            "uptime_s": int(now - self.started_at),
            "today": self.today,
            "count": self.today_count,
            "vision_count": self.today_vision,
            "persons_now": self.persons_now(),
            "zone_count": self.zone_count,
            "cam_count": self.cam_count,
            "active_events": len(self.active),
            "hold_ok": round(self.hold_ok, 1),
            "arrived": self.arrived,
            "asked": self.asked_names,
            "last_ask": (datetime.fromtimestamp(self.last_ask).strftime("%H:%M:%S")
                         if self.last_ask else None),
            "asking": self.asking,
            "llm": "on" if (self.cfg.get("llm") or {}).get("enabled") else "off",
            "last_door": (datetime.fromtimestamp(self.last_door_ts).strftime("%H:%M:%S")
                          if self.last_door_ts else None),
            "door_quiet_left_s": int(_left),
        }
        try:
            self.mq.publish(MQTT_STATE_TOPIC,
                            json.dumps(payload, ensure_ascii=False), retain=True)
        except Exception as e:
            log.debug("状态发布失败：%s", e)

    def _on_cmd(self, cmd):
        """远程指令。payload 形如：restart / status / test-ask / test-word /
        test-vision / speak:你好"""
        low = cmd.lower()
        log.info("📨 收到远程指令：%s", cmd)
        if low in ("restart", "reload", "exit", "quit"):
            self.publish_state(force=True)
            log.info("收到重启指令 → 退出进程；容器 restart 策略会立刻拉起，"
                     "并重新解析 bind mount（换过目录就靠这一次）")
            time.sleep(0.5)
            os._exit(0)
        if low in ("status", "state"):
            self.publish_state(force=True)
            return
        if low in ("test-vision", "vision"):
            threading.Thread(target=lambda: self._cmd_vision(), daemon=True).start()
            return
        if low in ("test-word", "word"):
            self.ask(mode="word", force=True)
            return
        if low in ("test-ask", "ask"):
            self.ask(mode="auto", force=True)
            return
        if low.startswith("speak:"):
            self.ha.speak(cmd.split(":", 1)[1])
            return
        log.warning("不认识的指令：%s（可用：restart/status/test-vision/test-word/test-ask/speak:文本）",
                    cmd)

    def _cmd_vision(self):
        who = self.next_kid() or (list(self.cfg.get("kid_grade") or {}) or [None])[0]
        q = self.pick_vision_question(who)
        if not q:
            log.warning("远程视觉测试失败：取帧或模型不可用")
            return
        text, ans, extra, tag = self.compose("vision", q, who, self.persons_now() or 1)
        log.info("远程视觉测试 ✅ %s | 场景=%s | 题=%s→%s", tag, q["scene"], q["zh"], ans)
        self.ha.speak(text)

    def persons_now(self):
        """当前客厅人数 = zone / 摄像头视野 / 事件流 三路取最大值。

        v0.3.4 起：以前是「zone_count 有值就只用 zone_count」，坑很深 ——
        Frigate 的 zone 坐标是占位示例没校准过，人一坐到餐桌边（检测框 y≈0.89，
        而 zone_center 只覆盖到 0.85）就整框落在 zone 外 → zone 计数恒为 0。

        ⚠️ 2026-10-01 实测再补一刀：这台 Frigate **根本不发布
        `frigate/<camera>/<object>/active` 主题**（旁听 25 秒只看到
        `frigate/c300/zone/zone_center/state`），所以 zone_count / cam_count
        恒为 None，真正管用的只有 `frigate/events` 事件流 —— 而它在 max 模式下
        也**不再要求命中 zone**（见 _on_message 注释）。
        count_mode: "zone_only" 可切回严格语义。
        """
        if self.simulate:
            return self.simulate
        zone_only = str(self.cfg["frigate"].get("count_mode", "max")).lower() == "zone_only"
        cands = []
        if self.zone_count is not None:
            cands.append(self.zone_count)
        if not zone_only and self.cam_count is not None:
            cands.append(self.cam_count)
        with self.lock:
            cands.append(len(self.active))
        return max(cands) if cands else 0

    def _who_in_msg(self, msg):
        """从米家消息文本里抠出是哪个在册孩子开的门。

        v0.4.8 抽出。原来的写法是：
            next((v for v in self.cfg["kids"].values() if v in msg), "")
        有两个隐患：
          ① 字典序遍历，若两个名字有包含关系（如「许子」vs「妹妹」），
             会命中先遍历到的那个 —— 应该**优先匹配更长的名字**；
          ② 家长的名字（奶奶/我）不在 kids 里，本来就不该匹配上，
             这里只要返回空就表示"不是在册孩子开的门"。
        """
        names = [v for v in (self.cfg["kids"] or {}).values() if v]
        # 长名优先：避免「许子」吃掉「妹妹」
        for name in sorted(set(names), key=len, reverse=True):
            if name in msg:
                return name
        return ""

    # -- 门锁 ---------------------------------------------------------------
    def _mark_door(self, who, source="", ours=True):
        """记一次"门外开锁" → 进入避让窗口（安静 arrival_quiet_minutes 分钟）。

        ★ v0.4.6 抽出来给两个源共用（门锁传感器 + 米家消息）。
        """
        lp = self.cfg.get("lock_parallel") or {}
        quiet = float(lp.get("arrival_quiet_minutes", 0) or 0)
        if quiet <= 0:
            return
        if not ours and not lp.get("quiet_on_any_unlock", True):
            return
        now = time.time()
        # 同一件事被两个源各报一次（差几秒），别重复刷日志/写盘
        if self.last_door_ts and (now - self.last_door_ts) < 20:
            return
        self.last_door_ts = now
        self._save_state()                  # v0.4.5 立刻落盘，重启不丢避让窗口
        log.info("🚪 门外开锁（%s，来源 %s）→ 让小爱先播英语，考官安静 %.0f 分钟",
                 who or "?", source or "-", quiet)

    def poll_lock(self):
        try:
            st = {s["entity_id"]: s for s in self.ha.states()}
        except Exception as e:
            log.debug("取 HA 状态失败：%s", e)
            return

        # ① 门锁传感器（sensor.lumi_wbmcn1_XXXX_lock_action）
        la = st.get(self.ha.lock_action)
        if la is not None:
            act = str(la.get("state", "")).strip()
            prev_act = self.last_lock_action
            self.last_lock_action = act
            if act == "outside_unlock" and prev_act not in (None, "outside_unlock"):
                kid = str((st.get(self.ha.lock_key_id) or {}).get("state", "")).strip()
                self._mark_door(self.cfg["kids"].get(kid, kid), "门锁传感器",
                                ours=kid in self.cfg["kids"])

        # ①-b ★v0.4.6 米家消息兜底 —— 这个才是真正实时的源
        #   实测（2026-10-01）：sensor.lumi_wbmcn1_XXXX_lock_action 是**云轮询**，
        #   根本不实时。13:46 孩子指纹「门外开锁」，米家消息实体秒级更新、
        #   也成功触发了 HA 自动化；但 lock_action 的最后一次变化停在 10:46:41，
        #   于是 kaoguan 完全没感知到开门（state 里 last_door=null），避让没生效。
        #   同一个毛病还让 lock_key_id 一直卡在 0 → arrived 也永远记不上。
        #   所以这里直接读米家消息文本，并顺手从文本里抠出是谁回来了。
        me = st.get(self.ha.mi_msg) if self.ha.mi_msg else None
        if me is not None:
            msg = str(me.get("state") or "").strip()
            if msg and msg != self.last_mi_msg:
                self.last_mi_msg = msg
                if "门外开锁" in msg:
                    # 消息实体被摄像头消息反复覆盖 → 只有"新鲜"的才算真事件
                    age = _age_seconds(me.get("last_changed") or me.get("last_updated"))
                    if age is None or age <= 120:
                        who = self._who_in_msg(msg)
                        ours = bool(who)
                        if who:
                            self.arrived.setdefault(who, datetime.now().strftime("%H:%M:%S"))
                            self._save_state()
                            log.info("🚪 %s 回家了（米家消息：%s）", who, msg)
                        else:
                            # ★v0.4.8：认不出是谁也记一条 —— 以前这里静默跳过，
                            #   导致"娃开门了但没记上家门"这类问题完全查不出原因。
                            #   现在至少能看到原始文本，方便回头对照 kids 映射。
                            log.info("🚪 门外开锁但没匹配到在册孩子，原文：%s", msg)
                        self._mark_door(who, "米家消息", ours=ours)

        # ② 记录今天谁回过家（门锁传感器那一路，能拿到就记）
        s = st.get(self.ha.lock_key_id)
        if not s:
            return
        kid = str(s.get("state", "")).strip()
        prev, self.last_key_id = self.last_key_id, kid
        if prev is not None and kid != prev and kid in self.cfg["kids"]:
            name = self.cfg["kids"][kid]
            self.arrived.setdefault(name, datetime.now().strftime("%H:%M:%S"))
            log.info("🚪 %s 回家了（凭证 %s）", name, kid)
            self._save_state()

    # -- 闸门 ---------------------------------------------------------------
    def _window_block(self, now):
        """返回被时段拦住的原因（None=放行）。"""
        t = now.strftime("%H:%M:%S")
        w = self.cfg["window"]
        if not (w["after"] <= t <= w["before"]):
            return "不在允许时段 %s~%s" % (w["after"], w["before"])
        for a, b in (w.get("mute") or []):
            if a <= t <= b:
                return "处于静音段 %s~%s（总窗口 %s~%s）" % (a, b, w["after"], w["before"])
        return None

    def _in_window(self, now):
        return self._window_block(now) is None

    def _rollover_if_needed(self, now):
        """跨自然日就把今日计数清零。

        ★ v0.4.7 修：原版只在 tick() 里查跨天，而 tick() 一旦被闸门拦下就
        ，**在 pending_at 排定后到开口前这段（最长 random_delay_max 秒 +
        出题线程 8~30s）跨零点，就会带着昨天的 count 和 pending_at 接着走** ——
        实测 2026-10-02 00:19:51 的那一题正是这么冒出来的（state.json date 已是
        10-02，count 却从 0 直接记成 1，出题人自己都没收到"跨天清零"那条日志）。
        出题前统一再滚一次日期，代价是几纳秒，换来日期口径永远自洽。
        """
        d = now.strftime("%Y-%m-%d")
        if d == self.today:
            return
        log.info("跨天（出题前复查）：%s -> %s，今日计数/回家名单清零", self.today, d)
        self.today = d
        self.today_count = 0
        self.today_vision = 0
        self.arrived = {}
        self.asked_names = []
        self.last_ask = None
        self._save_state()

    def _quiet_left(self):
        """门口刚开锁后还要安静多少秒（0 = 不避让）。"""
        lp = self.cfg.get("lock_parallel") or {}
        quiet = float(lp.get("arrival_quiet_minutes", 0) or 0)
        if quiet <= 0 or not self.last_door_ts:
            return 0.0
        return max(0.0, quiet * 60 - (time.time() - self.last_door_ts))

    def _can_ask(self, now):
        self._rollover_if_needed(now)
        lim, trg = self.cfg["limits"], self.cfg["trigger"]
        if self.today_count >= lim["max_per_day"]:
            # ★ v0.4.9 额度到顶后彻底静默。之前每个 tick 都打一行
            #   「时机不合适：今日已达上限 N 次」—— 实测 2026-10-04 从 12:49 到
            #   19:23 刷了 600+ 行纯噪音，把真正该看的（出题/门锁/异常）全淹了。
            #   现在到顶那一刻打一条醒目日志（带次日恢复时间点），之后不再重复打；
            #   跨天由 _rollover_if_needed 自然解除。
            if not getattr(self, "_quota_logged", False):
                self._quota_logged = True
                _nxt = (now.replace(hour=0, minute=0, second=0, microsecond=0)
                        + timedelta(days=1))
                log.info("🛑 今日额度用满（%d/%d 次），小考官今天收工、不再出题，"
                         "下次恢复 %s。今日已问：%s",
                         self.today_count, lim["max_per_day"],
                         _nxt.strftime("%m-%d %H:%M"),
                         "、".join(self.asked_names or []) or "无")
            return False, "今日已达上限 %d 次" % lim["max_per_day"]
        self._quota_logged = False
        if self.last_ask:
            gap = (now - datetime.fromtimestamp(self.last_ask)).total_seconds() / 60
            if gap < lim["min_gap_minutes"]:
                return False, "距上次仅 %.0f 分钟" % gap
        blk = self._window_block(now)
        if blk:
            return False, blk
        # ★ 与门锁并行：门口刚开锁，让小爱先把英语播完
        left = self._quiet_left()
        if left > 0:
            return False, "刚有人开锁，让小爱先播英语（还差 %.0f 分钟）" % (left / 60.0)
        need = trg.get("require_kids_home", 0)
        if need and len(self.arrived) < need:
            return False, "今天只有 %d 个娃回过家（需要 %d）" % (len(self.arrived), need)
        return True, ""

    # -- 判定主循环 ---------------------------------------------------------
    def tick(self):
        now = datetime.now()
        d = now.strftime("%Y-%m-%d")
        if d != self.today:
            log.info("跨天，计数清零")
            self.today, self.today_count, self.arrived, self.asked_names = d, 0, {}, []
            self.today_vision = 0
            self.last_ask = None
            self._save_state()

        # ★ v0.4.9 额度到顶后**整体短路**，不再跑人数累计。
        #   之前即使 _can_ask 恒返 False，前面那段「⏳ 客厅累计达标 x s / 40 s」
        #   还是会每 7 秒刷一行 —— 实测 2026-10-04 额度一满就是 600+ 行噪音。
        #   到顶 = 今天收工，没什么好观察的，直接把 tick 关掉最省事。
        if self.today_count >= self.cfg["limits"]["max_per_day"]:
            self.tick_at = None
            self.hold_ok = 0.0
            self.last_ok_at = None
            self.hold_milestone = 0
            self.pending_at = None
            return

        n = self.persons_now()
        trg = self.cfg["trigger"]
        need = trg["min_persons"]
        hold = trg["hold_seconds"]
        grace = trg.get("hold_grace_seconds", 20)
        ts = time.time()
        dt = 0.0 if self.tick_at is None else max(0.0, min(ts - self.tick_at, 5.0))
        self.tick_at = ts

        # ── 滑动窗口判定：累计达标时长，掉线在 grace 内不清零 ──
        if n >= need:
            if self.hold_ok <= 0:
                log.info("👀 客厅有 %d 人（需要 ≥%d），开始累计（宽限 %.0fs，门槛 %.0fs）",
                         n, need, grace, hold)
                self.hold_milestone = 0
            self.hold_ok += dt
            self.last_ok_at = ts
        elif self.last_ok_at is None or ts - self.last_ok_at > grace:
            if self.hold_ok > 0:
                log.info("客厅人数低于 %d 已超 %.0fs，累计归零（丢弃 %.0fs）",
                         need, grace, self.hold_ok)
            self.hold_ok = 0.0
            self.last_ok_at = None
            self.hold_milestone = 0
            if self.pending_at:
                log.info("人数不足，取消已排定的开口")
            self.pending_at = None
            return

        if self.hold_ok < hold:
            step = max(5.0, hold / 6.0)
            ms = int(self.hold_ok // step) * step
            if ms > self.hold_milestone:
                self.hold_milestone = ms
                log.info("⏳ 客厅累计达标 %.0fs / %.0fs（此刻 %d 人）", self.hold_ok, hold, n)
            return

        if ts < self.reject_until:
            return

        if self.pending_at is None:
            ok, why = self._can_ask(now)
            if not ok:
                log.info("⏸ 时机不合适：%s（%d 秒后再看）", why, REJECT_COOLDOWN)
                self.reject_until = ts + REJECT_COOLDOWN
                self.hold_ok = 0.0
                self.last_ok_at = None
                self.hold_milestone = 0
                return
            delay = random.randint(0, trg["random_delay_max"])
            self.pending_at = ts + delay
            faces = self.faces_now()
            log.info("⏰ 客厅累计达标 %.0fs（门槛 %.0fs，此刻 %d 人）；今日回家：%s；人脸刚认出：%s；%.0fs 后开口",
                     self.hold_ok, hold, n, "、".join(self.arrived) or "（无）",
                     "、".join(faces) or "（无）", delay)
            return

        if ts >= self.pending_at:
            self.pending_at = None
            if self.asking:
                log.info("上一题还在出，跳过这一轮")
                return
            # ★ 出题要调视觉模型（8~30s），必须放到独立线程，否则主循环被卡死
            with self.lock:
                self.asking = True
            threading.Thread(target=self._ask_job, daemon=True).start()

    def _ask_job(self):
        try:
            self.ask()
        except Exception as e:
            log.error("出题线程异常：%s", e)
        finally:
            self.asking = False

    def faces_now(self):
        """最近 FACE_FRESH 秒内人脸认出过的人"""
        now = time.time()
        with self.lock:
            return [n for n, t in self.seen_faces.items() if now - t <= FACE_FRESH]

    def next_kid(self, pool=None):
        """挑一个孩子来点名。

        `pool` 可由调用方传入，避免在 `ask()` 里就要打日志时重复查一次
        Frigate —— 两次查询相隔几秒，人数可能已变，会让「日志里的候选池」
        和「实际点名用的池」对不上，事后复盘很坑。

        v0.4.8 起的三条规则（兴哥 2026-10-03 定）：

        ① **候选池 = 人脸认出的 ∪ 今天回过家的**（见 `present_kids()`）。
           人脸识别在这台设备上长期失效（`sub_label` 恒为 None），所以实际
           主要靠门锁（米家消息里的「XXX 门外开锁」）填 `arrived`。
           画面 0 人时 `present_kids()` 直接返回空 —— 客厅没人绝不出题。

        ② **随机挑，但同一人连续被点 2 次就强制换人。**
           纯随机在只有 2 个候选人时会频繁连点同一个人（连两次概率 1/4），
           孩子会觉得"怎么老问我"。

        ③ **今天没被点过的优先 —— 但只作为"加权"，绝不排除别人。**
           ⚠️ 这里踩过一个坑（2026-10-03 实测发现）：
             最初写成「fresh = [没点过的] 直接替换候选池」，两个娃时
             `asked_names=['姐姐']` → fresh 只剩 `[妹妹]` → 候选池变成 1 人 →
             不仅**永远只点妹妹**，还让规则②的"连二次必换"失效
             （`len(pool) > 1` 不成立）。实测连点 12 次全是同一人。
           正确做法：没点过的**加权**（高概率优先），但已点过的仍在池子里，
           保证"随机"和"换人"都还成立。
        """
        if pool is None:
            pool = self.present_kids()
        if not pool:
            return None

        if len(pool) == 1:
            who = pool[0]
            self.recent_picks.append(who)
            self.recent_picks = self.recent_picks[-6:]
            return who

        # ② 连二次必换：把"刚连点 2 次的人"降权（不是删除 —— 只剩 1 人时还得能选）
        last2 = self.recent_picks[-2:] if len(self.recent_picks) >= 2 else []
        just_twice = last2[0] if last2 and last2[0] == last2[1] else None

        # ③ 没点过的加权
        weights = []
        for n in pool:
            w = 1.0
            if n not in self.asked_names:
                w *= 3.0                     # 今天没点过的，权重 ×3
            if n == just_twice:
                w *= 0.01                    # 刚连点 2 次的，权重砍到几乎不选
            weights.append(w)

        who = random.choices(pool, weights=weights, k=1)[0]
        self.recent_picks.append(who)
        self.recent_picks = self.recent_picks[-6:]      # 只留最近 6 次
        return who

    def present_kids(self):
        """推断「此刻可能在客厅的孩子」集合。

        v0.4.8 新增（兴哥 2026-10-03 定的口径：**门锁 + Frigate 一起用**）。

        ── 为什么要这个方法 ────────────────────────────────────
        以前的 `next_kid()` 写的是 `self.faces_now() or self.arrived`，有两个问题：
          · 人脸识别在这台设备上**长期失效**（`sub_label` 恒为 None），
            `faces_now()` 永远是空 → 实际只用到了 `arrived`，
            但代码形态上看不出这一点，容易误判；
          · 两个娃都在家时，没法表达"候选人有两个"，日志里也看不出来。

        ── 两个数据源各自的可靠性（实测结论）────────────────────
        | 源 | 给什么 | 可靠性 |
        |---|---|---|
        | 门锁（米家消息 → arrived） | **谁**回来过 | 身份准，但只有"进门那一刻"，且只有开锁的人 |
        | Frigate 人数（persons_now） | **有几个人** | 数得还算准，但**不知道是谁** |
        | 人脸（seen_faces） | 谁 | 目前恒空（未修复），修好自动生效 |

        两者天生互补：门锁给身份不给在场数，Frigate 给在场数不给身份。
        **合起来才够用** —— 这正是兴哥说的"他们两个人一起回来一个人开门很正常"：
        一次开锁只录到一个名字，但画面里是两个人。

        ── 规则 ──────────────────────────────────────────────
        ① **画面 0 人 → 直接返回空**。
           这是防误报的硬底线：`arrived` 记的是"今天回来过"，不是"现在在家"。
           娃早上出门上学，名字还留在 `arrived` 里；若拿它当在场，
           客厅空着也会出题，孩子不在家却被点名，很尴尬。

        ② 画面 ≥1 人时：`arrived`（∪ 人脸）里的人都算候选。
           画面只数到 1 人但 `arrived` 有 2 个 → **仍给 2 个**，
           因为"两人同行、只一人开锁"是常态。
        """
        n = self.persons_now()
        # ① 画面一个人都没有 → 客厅没人，绝不出题（哪怕 arrived 里有名字）
        if n < 1:
            return []

        faces = self.faces_now()
        arrived = list(self.arrived.keys())

        pool = []
        for name in faces + arrived:
            if name and name not in pool:
                pool.append(name)

        # ★ 画面里有人但门锁/人脸都说不出是谁 → 不硬猜，返回空
        #    （宁可不出题，也不要张冠李戴点错人）
        if not pool:
            log.debug("画面 %d 人，但门锁/人脸都没给出身份，本轮不点名", n)
            return []

        if len(pool) > 1:
            log.debug("画面 %d 人，候选 %d 个（%s）", n, len(pool), "、".join(pool))
        return pool

    # -- 出题 ---------------------------------------------------------------
    OPENERS = [
        "叮咚～ 英语小考官上线！",
        "咳咳，本考官路过客厅，发现{who}！",
        "检测到小朋友出没，临时抽查开始——",
        "喂喂，{who}，玩得挺开心啊？来，答一题再继续！",
    ]
    PRAISE = ["答对了拍拍手 👏", "答对啦，真棒！", "不错不错～", "这题没难住啊！"]
    ENCOURAGE = ["没关系，这题有点小坑，记住就好。", "差一点点，下次就记住了。",
                 "不要紧，听一遍就赚了。", "这个容易混，咱们记一下。"]
    NO_ANSWER = ["好像没答上来？那我直接说答案。", "没听清你说，我公布答案啦——",
                 "这题先欠着，答案先收下："]

    @staticmethod
    def _who_phrase(n):
        names = {1: "一个小家伙", 2: "两名可疑人员", 3: "三名同学", 4: "四位同学"}
        return names.get(n, "一屋子同学" if n > 4 else "小朋友")

    def compose(self, kind, payload, who, n_persons):
        """把素材拼成 (question_text, answer_text, tag)"""
        d = self.cfg["trigger"]["answer_delay_seconds"]
        parts = [random.choice(self.OPENERS).format(who=self._who_phrase(n_persons))]

        if kind == "vision":
            v = payload
            if v.get("scene"):
                parts.append("我看看啊——%s。" % v["scene"].rstrip("。"))
            parts.append("%s，画面里的『%s』用英语怎么说？" % (who or "小朋友", v["zh"]))
            hint = v.get("hint") or ""
            if v.get("from_list"):
                hint = (hint + "，" if hint else "") + "这个词你课本上学过哦"
            if hint:
                parts.append("提示：%s。" % hint)
            parts.append("给你 %d 秒，不许查手机哦～" % d)
            text = "".join(parts)
            ans = v["word"]
            extra = v.get("sentence") or ""
            tag = "看画面(%s via %s %.1fs)" % (
                v.get("model", "?"), v.get("via") or "?", v.get("cost_s") or 0)
            return text, ans, extra, tag

        if kind == "word":
            w = payload
            parts.append("%s，考你一个：" % who if who else "考你们一个：")
            parts.append("『%s』用英语怎么说？" % w["zh"])
            if w.get("hint"):
                parts.append("提示：%s。" % w["hint"])
            # ★ v0.7.0：高年级多考一步 —— 光会念不算会，得能放进句子里（时态要对）。
            tag = "年级词表"
            gnum = self._grade_num(w.get("grade") or self.grade_of(who))
            ask_from = int((self.cfg.get("sentence") or {}).get("ask_kid_from_grade", 6) or 0)
            if gnum and ask_from and gnum >= ask_from:
                parts.append("说出这个单词，再用它造一个完整的句子。")
                tag = "年级词表+造句"
            parts.append("给你 %d 秒，不许查手机哦～" % d)
            # extra = 例句，报答案时由 _reveal 一起念出来
            return "".join(parts), w["word"], (w.get("sentence") or ""), tag

        group, item = payload
        q, a = item.get("q", ""), item.get("a", "")
        hint = item.get("hint") or ""
        style = item.get("style") or self.cfg["group_style"].get(group, "plain")
        parts.append("%s，考你一个：" % who if who else "考你们一个：")
        if style == "en":
            parts.append("『%s』用英语怎么说？" % q)
        elif style == "zh":
            parts.append("『%s』是什么意思？" % q)
        else:
            parts.append("%s？" % q)
        if hint:
            parts.append("提示：%s。" % hint)
        parts.append("给你 %d 秒，不许查手机哦～" % d)
        return "".join(parts), a, "", "%s组" % group

    def ask(self, force=False, mode="auto"):
        # ★ v0.4.3 闸门二次确认。
        #   tick() 的流程是「先 _can_ask 通过 → 记下 pending_at → 过 random_delay_max 秒
        #   再开口」，中间这段时间里 _can_ask 的结果可能已经失效（刚有人开锁、跨到
        #   静音段、上一题其实刚问过…）；而且 _ask_job 是独立线程，和主循环并行。
        #   实测就撞上过：state.json 里 count=2 但两次开口只隔十几分钟，按 25 分钟
        #   间隔是绝不该发生的 —— 说明确实有一条绕过闸门的路径。
        #   这里在**真正开口前**再查一次，闸门才算权威。手动指令（MQTT test-*）传
        #   force=True 跳过，那个是给人调试用的。
        if not force:
            ok, why = self._can_ask(datetime.now())
            if not ok:
                log.warning("🚫 开口前复查没过，本次不出题：%s", why)
                return False
        else:
            self._rollover_if_needed(datetime.now())
        # ★v0.4.8：把"两人都在"这个场景显式记进日志 —— 之前日志里只有人数和
        #   被点名的人，看不出"候选池里其实有两个娃"，事后复盘很费劲。
        #   先在**同一次**查询里把 pool 定下来，再交给 next_kid() 复用。
        pool = self.present_kids()
        who = self.next_kid(pool=pool)
        n = self.persons_now()
        if len(pool) > 1:
            log.info("👥 候选 %d 人（%s），画面 %d 人 → 本次点名 %s",
                     len(pool), "、".join(pool), n, who or "无")
        kind, payload = self._pick_material(who, mode=mode)
        if kind is None:
            log.warning("三种出题方式都没拿到素材，本次跳过")
            return False

        text, answer, extra, tag = self.compose(kind, payload, who, n)
        self.ha.speak(text)

        self.today_count += 1
        if kind == "vision":
            self.today_vision += 1
        self.last_ask = time.time()
        if who and who not in self.asked_names:
            self.asked_names.append(who)
        self._save_state()
        # ★ 2026-10-19：出过的题落盘 —— 这是词级去重的唯一依据。
        #   以前只有「今日第几次」，没有「出过什么词」，所以 book/desk 天天重来。
        try:
            if isinstance(payload, dict):
                self.record_quiz(who, kind, payload.get("word", ""), payload.get("zh", ""),
                                 payload.get("model", ""))
            elif isinstance(payload, (tuple, list)) and len(payload) == 2:
                # ★ v0.7.0：题库题也把答案记进去，否则历史里全是空 word，
                #   事后查「题库重复了几次」完全看不见（10-21~11-01 有 12 条这样的空白记录）。
                it = payload[1] if isinstance(payload[1], dict) else {}
                self.record_quiz(who, kind, it.get("a", ""), it.get("q", ""), "")
            else:
                self.record_quiz(who, kind, "", "", "")
        except Exception as e:
            log.warning("写出题历史失败（不影响出题）：%s", e)

        # ★ v0.4.9：报答案的计时**从「大约念完」起算**，不从现在起算。
        #   原版直接 Timer(answer_delay_seconds) → 念题本身要 20 秒左右，
        #   计时 15 秒先到期 → 题目被答案打断（2026-10-03 兴哥实听反馈）。
        #   现在：先等 speak_s（估算的播报时长），再等 answer_delay_seconds
        #   （留给孩子思考的时间）。
        think = self.cfg["trigger"]["answer_delay_seconds"]
        speak_s = self._speak_seconds(text)
        total = speak_s + think
        log.info("📝 已出题（%s → %s，今日第 %d 次，其中看画面 %d 次）：%s → %s"
                 "（念题约 %.0fs + 思考 %ds = %.0fs 后报答案）",
                 tag, who or "随机", self.today_count, self.today_vision,
                 (payload.get("zh") if kind in ("vision", "word") else payload[1].get("q")),
                 answer, speak_s, think, total)
        # ★ 2026-10-19：开一个「听答案」窗口 —— 从念完题开始，到报答案前 2 秒结束。
        #   必须早于 _reveal，否则小爱报答案那声会被当成孩子答对。
        if kind in ("vision", "word") and isinstance(payload, dict):
            _w = payload.get("word") or ""
            _z = payload.get("zh") or ""
            if _w:
                t0 = time.time()
                threading.Thread(
                    target=self._watch_answer,
                    args=(who, _w, _z, t0 + speak_s,
                          t0 + max(speak_s + 1, total - 2)),
                    daemon=True).start()
        threading.Timer(total, self._reveal, args=(answer, extra, answer)).start()
        return True

    @staticmethod
    def _spell(ans):
        """中文 TTS 念英文单词常常含糊，报答案时顺带把拼写念出来。"""
        if not ans or not re.fullmatch(r"[A-Za-z]+(?: [A-Za-z]+)*", ans.strip()):
            return ""
        return "、".join("-".join(list(w)) for w in ans.split())

    def _speak_seconds(self, text):
        """估算一句 TTS 要念多久（秒）。

        ★ v0.4.9 新增。为什么需要它：

        `threading.Timer` 原来是从「发出题」那一刻起算 `answer_delay_seconds`，
        但小爱念题本身就要十几二十秒 —— 于是**题还没念完，报答案就下来了**，
        直接把题目打断（2026-10-03 兴哥实听反馈）。

        HA 侧**拿不到**「小爱念完没有」这个信号（实测：写入 play_text 后 state
        立刻被清空、media_player 恒为 playing，没有任何「播报结束」事件），
        所以只能按文本长度估。中文 TTS 约 **4.5 字/秒**，含标点停顿。

        调用方用它把计时起点后移到「大约念完」之后。
        """
        if not text:
            return 0.0
        cps = float(self.cfg["trigger"].get("tts_chars_per_second", 4.5)) or 4.5
        return len(text) / cps

    def _reveal(self, answer, extra="", word=None):
        sp = self._spell(answer)
        tail = "拼写：%s。" % sp if sp else ""
        extra = (extra or "").strip()
        # 英文例句自带句点，别再加个中文句号变成 ".。"
        mid = extra + ("" if extra[-1] in "。.！!？?" else "。") if extra else ""
        # ★ v0.7.1：按「这题到底答上没答上」选前缀，不再一律夸。
        ok, why = self._judged_of(word or answer)
        if ok is True:
            head = random.choice(self.PRAISE)
        elif ok is False:
            head = random.choice(self.ENCOURAGE)
        else:
            head = random.choice(self.NO_ANSWER)
        self.ha.speak(head +
                      " 答案是 %s。%s%s下一题一会儿再来～" % (answer, tail, mid))
        log.info("✅ 已报答案：%s%s%s（判定 %s）", answer,
                 ("（拼写 %s）" % sp) if sp else "",
                 ("（例句 %s）" % extra) if extra else "", why)

    def _judged_of(self, word):
        """取这道题的判定结果 → (True/False/None, 说明文字)。

        ★ v0.7.1 新增。没有这道题的记录（比如远程测试出题、答案监视未开启）
        就返回 None，报答案时用中性说法，不假装孩子答对了。
        """
        lj = getattr(self, "_last_judged", None) or {}
        if not lj:
            return None, "无记录"
        # 只认最近 10 分钟内的判定，避免上一题的结论串到这一题
        if time.time() - float(lj.get("ts") or 0) > 600:
            return None, "记录过期"
        w = (word or "").strip().lower()
        if w and lj.get("word") and lj.get("word") != w:
            return None, "词对不上(%s)" % lj.get("word")
        ok = lj.get("ok")
        return ok, {True: "对", False: "错"}.get(ok, "没答")

    # -- 主循环 -------------------------------------------------------------
    def run(self):
        self.start_mqtt()
        llm = self.cfg.get("llm") or {}
        log.info("小考官 %s 已启动（dry-run=%s simulate=%d）；看画面出题=%s（%s）；Ctrl-C 退出",
                 VERSION, self.dry, self.simulate,
                 "开" if llm.get("enabled") else "关",
                 ",".join(llm.get("models") or []) or "未配模型")
        last_poll = 0
        try:
            while True:
                time.sleep(1)
                if time.time() - last_poll >= 5:
                    last_poll = time.time()
                    self.poll_lock()
                self.tick()
                self.publish_state()
        except KeyboardInterrupt:
            log.info("退出")
        finally:
            try:
                self.mq.loop_stop()
            except Exception:
                pass


# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(BASE, "config.yaml"))
    ap.add_argument("--dry-run", action="store_true", help="只判定不播报")
    ap.add_argument("--simulate", type=int, default=0, metavar="N",
                    help="假装客厅有 N 个人（端到端测试用）")
    ap.add_argument("--test-ask", action="store_true", help="立刻出一次题（三分支自动）")
    ap.add_argument("--test-word", action="store_true", help="立刻从年级词表出一题")
    ap.add_argument("--test-vision", action="store_true",
                    help="立刻走一次看画面出题，打印取帧结果与模型返回（不播报）")
    ap.add_argument("--speak", action="store_true",
                    help="配合 --test-vision：把生成的题真的念出来")
    ap.add_argument("--list-words", action="store_true", help="只列出读到的词表")
    ap.add_argument("--test-speak", metavar="TEXT", help="直接播一句验证通道")
    ap.add_argument("--state", action="store_true",
                    help="打印当前生效的关键配置（不含密钥）")
    ap.add_argument("--test-pick", type=int, default=0, metavar="N",
                    help="模拟点名：假设客厅 N 人、arrived 为指定名单，"
                         "连点 12 次看轮换效果（不播报、不联网）")
    ap.add_argument("--arrived", default="",
                    help="配合 --test-pick：逗号分隔的今日回家名单，如 姐姐,妹妹")
    ap.add_argument("--client-id", metavar="ID",
                    help="覆盖 MQTT client_id（本机起第二个实例做测试时用，"
                         "别和容器里的 'kaoguan' 撞名，否则会把容器挤下线）")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    cfg = load_cfg(args.config)
    if args.client_id:
        cfg["mqtt"]["client_id"] = args.client_id
    k = Kaoguan(cfg, dry=args.dry_run, simulate=args.simulate)

    if args.test_pick:
        names = [x.strip() for x in (args.arrived or "").split(",") if x.strip()]
        k.arrived = {n: "00:00:00" for n in names}
        k.simulate = args.test_pick
        print("模拟：客厅 %d 人，arrived=%s" % (args.test_pick, "、".join(names) or "（空）"))
        print("候选池 present_kids() = %s" % ("、".join(k.present_kids()) or "（空）"))
        print("连续点名 12 次：")
        seq = []
        for i in range(12):
            w = k.next_kid()
            seq.append(w or "（不点名）")
            print("  %2d. %s" % (i + 1, w or "（不点名）"))
        from collections import Counter
        c = Counter(x for x in seq if x != "（不点名）")
        print("分布：%s" % dict(c))
        # 检查有没有出现"同一人连 3 次"
        bad = [i for i in range(2, len(seq)) if seq[i] == seq[i-1] == seq[i-2]]
        print("连 3 次同一人的位置：%s" % (bad or "无 ✅"))
        return

    if args.state:
        llm = cfg.get("llm") or {}
        cam = cfg.get("camera") or {}
        trg = cfg["trigger"]
        print("版本      : %s" % VERSION)
        print("年级映射  : %s" % json.dumps(cfg.get("kid_grade") or {}, ensure_ascii=False))
        print("看画面出题: %s  models=%s" % ("开" if llm.get("enabled") else "关",
                                          ",".join(llm.get("models") or []) or "无"))
        print("模型网关  : %s" % llm.get("base_url"))
        print("取帧地址  : %s" % cam.get("snapshot_url"))
        print("出题比例  : 看画面 %s%% / 年级词表 %s%% / 其余走题库" % (
            (cfg.get("quiz") or {}).get("vision_ratio", 0),
            (cfg.get("quiz") or {}).get("words_ratio", 0)))
        print("触发条件  : 客厅≥%s 人，累计 %ss（宽限 %ss）" % (
            trg["min_persons"], trg["hold_seconds"], trg.get("hold_grace_seconds")))
        print("时段      : %s~%s   静音段 %s" % (
            cfg["window"]["after"], cfg["window"]["before"],
            cfg["window"].get("mute") or "无"))
        print("频次限制  : 每天 %s 次，间隔 %s 分钟" % (
            cfg["limits"]["max_per_day"], cfg["limits"]["min_gap_minutes"]))
        print("门锁避让  : 开锁后安静 %s 分钟" % (
            (cfg.get("lock_parallel") or {}).get("arrival_quiet_minutes", 0)))
        print("人数口径  : %s" % cfg["frigate"].get("count_mode", "max"))
        for name, items in k.load_words().items():
            users = [n for n, g in (cfg.get("kid_grade") or {}).items() if g == name]
            print("词表      : %-8s %3d 词  %s" % (name, len(items),
                                                  ("← " + "、".join(users)) if users else ""))
        sys.exit(0)

    if args.test_speak:
        sys.exit(0 if k.ha.speak(args.test_speak) else 1)

    if args.list_words:
        words = k.load_words()
        print("词表目录：%s" % cfg["words_dir"])
        if not words:
            print("  （空 —— 把 <年级>.txt 放进去即可）")
        for name, items in words.items():
            users = [n for n, g in (cfg.get("kid_grade") or {}).items() if g == name]
            print("  %-14s %5d 个词    例：%-28s %s" % (
                name, len(items), "%s %s" % items[0],
                ("← " + "、".join(users)) if users else ""))
        print("\n人名 → 年级：%s" % json.dumps(cfg.get("kid_grade") or {}, ensure_ascii=False))
        sys.exit(0)

    if args.test_vision:
        who = args.test_word and "姐姐" or (list(cfg.get("kid_grade") or {}) or ["姐姐"])[0]
        print("测试对象：%s（%s）" % (who, k.grade_of(who)))
        img = k.grab_frame()
        print("取帧：%s" % ("%d 字节 ✅" % len(img) if img else "失败 ❌"))
        q = k.pick_vision_question(who)
        if not q:
            print("看画面出题失败 ❌（检查 camera.snapshot_url 与 llm 配置）")
            sys.exit(1)
        print("模型：%s   路由：%s   耗时：%ss" % (q["model"], q.get("via"), q["cost_s"]))
        print("场景：%s" % q["scene"])
        print("题目：『%s』用英语怎么说？   答案：%s" % (q["zh"], q["word"]))
        print("例句：%s" % q.get("sentence"))
        print("是否来自课本词表：%s" % ("是" if q.get("from_list") else "否（模型自由发挥）"))
        text, ans, extra, tag = k.compose("vision", q, who, k.persons_now() or 1)
        print("\n会这样念：\n%s" % text)
        if args.speak:
            k.ha.speak(text)
        sys.exit(0)

    if args.test_ask or args.test_word:
        k.arrived.setdefault("姐姐", datetime.now().strftime("%H:%M:%S"))
        k.asked_names = []
        mode = "word" if args.test_word else "auto"
        k.ask(mode=mode, force=True)
        # ★ v0.4.9：等够「念题 + 思考」再退出，否则没等到报答案就退出了。
        #   这里给个上限，避免估算过长时卡住终端。
        time.sleep(cfg["trigger"]["answer_delay_seconds"] + 40)
        sys.exit(0)
    k.run()


if __name__ == "__main__":
    main()
