from datetime import date, timedelta
from html import escape
import random
import re
import time
from typing import Any
from urllib.parse import urlencode

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

try:
    from aiohttp import web
except ImportError:
    web = None


@register("astrbot_plugin_fun", "Copilot", "聊天积分玩法插件", "1.1.0")
class PointsPlugin(Star):
    DATA_KEY = "points_data_v1"
    SPEECH_DAILY_RETENTION_DAYS = 62
    SPEECH_MONTHLY_RETENTION_MONTHS = 18
    DEFAULT_PRODUCT_SLOT_COUNT = 12
    DEFAULT_PRODUCT_SLOT_MAX = 30

    def __init__(self, context: Context, config: Any = None):
        super().__init__(context)
        self.config = config or {}
        self._dashboard_runner = None
        self._dashboard_url = ""
        self._dashboard_error = ""

    async def initialize(self):
        """插件初始化时自动调用。"""
        await self._start_dashboard_server()

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _cfg_int(self, key: str, default: int, low: int | None = None, high: int | None = None) -> int:
        value = default
        if isinstance(self.config, dict):
            value = self._to_int(self.config.get(key, default), default)
        if low is not None:
            value = max(low, value)
        if high is not None:
            value = min(high, value)
        return value

    def _cfg_str(self, key: str, default: str = "") -> str:
        if isinstance(self.config, dict):
            value = self.config.get(key, default)
            if value is None:
                return default
            return str(value)
        return default

    def _cfg_bool(self, key: str, default: bool) -> bool:
        value = default
        if isinstance(self.config, dict):
            value = self.config.get(key, default)

        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on", "y"}
        return bool(value)

    def _save_plugin_config(self) -> None:
        save_fn = getattr(self.config, "save_config", None)
        if callable(save_fn):
            try:
                save_fn()
            except Exception as exc:
                logger.warning(f"保存插件配置失败: {exc}")

    @staticmethod
    def _normalize_group_id(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if text.isdigit():
            return text
        match = re.search(r"\d{5,}", text)
        if match:
            return match.group(0)
        return text

    @staticmethod
    def _parse_id_text(value: Any, normalizer) -> list[str]:
        items: list[str] = []
        if isinstance(value, list):
            for item in value:
                items.append(str(item))
        else:
            text = str(value or "")
            items = re.split(r"[,，;；\n\r\t ]+", text)

        result: list[str] = []
        seen = set()
        for item in items:
            normalized = normalizer(item)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

    def _cfg_group_id_list(self, key: str) -> list[str]:
        raw = ""
        if isinstance(self.config, dict):
            raw = self.config.get(key, "")
        return self._parse_id_text(raw, self._normalize_group_id)

    def _set_group_id_list(self, key: str, values: list[str]) -> None:
        if isinstance(self.config, dict):
            self.config[key] = values
        self._save_plugin_config()

    @staticmethod
    def _get_event_group_id(event: AstrMessageEvent) -> str:
        message_obj = getattr(event, "message_obj", None)
        group_id = getattr(message_obj, "group_id", "")
        return str(group_id or "").strip()

    def _is_group_allowed_by_policy(self, group_id: str) -> bool:
        gid = self._normalize_group_id(group_id)
        if not gid:
            return False

        whitelist = set(self._cfg_group_id_list("group_whitelist"))
        blacklist = set(self._cfg_group_id_list("group_blacklist"))
        if gid in blacklist:
            return False
        if whitelist and gid not in whitelist:
            return False
        return True

    def _is_event_allowed(self, event: AstrMessageEvent) -> bool:
        group_id = self._get_event_group_id(event)
        if not group_id:
            return self._cfg_bool("group_enable_private", True)
        return self._is_group_allowed_by_policy(group_id)

    def _blocked_result(self, event: AstrMessageEvent):
        if self._is_event_allowed(event):
            return None
        return event.plain_result("当前群未开启积分功能，请联系管理员设置白名单/黑名单。")

    def _resolve_target_group(self, event: AstrMessageEvent, group_id: str = "") -> str:
        target = self._normalize_group_id(group_id)
        if target:
            return target
        return self._get_event_group_id(event)

    def _extract_message_components(self, event: AstrMessageEvent) -> list[Any]:
        message_obj = getattr(event, "message_obj", None)
        components = getattr(message_obj, "message", None)
        if isinstance(components, list):
            return components

        get_messages_fn = getattr(event, "get_messages", None)
        if callable(get_messages_fn):
            try:
                messages = get_messages_fn()
                if isinstance(messages, list):
                    return messages
            except Exception:
                pass
        return []

    def _is_message_to_bot(self, event: AstrMessageEvent) -> bool:
        # 私聊天然是和机器人聊天。
        if not self._get_event_group_id(event):
            return True

        message_obj = getattr(event, "message_obj", None)
        self_id = str(getattr(message_obj, "self_id", "")).strip()

        for comp in self._extract_message_components(event):
            qq = None
            if isinstance(comp, dict):
                if str(comp.get("type", "")).lower() == "at":
                    qq = comp.get("qq") or comp.get("id") or comp.get("target")
            else:
                if hasattr(comp, "qq"):
                    qq = getattr(comp, "qq")
                elif str(getattr(comp, "type", "")).lower() == "at":
                    qq = getattr(comp, "target", None) or getattr(comp, "id", None)

            if qq is None:
                continue
            if self_id and str(qq).strip() == self_id:
                return True

        # 兜底：兼容部分平台 raw_message 的 at 格式。
        raw_message = str(getattr(message_obj, "raw_message", ""))
        if self_id and (
            f"qq={self_id}" in raw_message
            or f'"qq":"{self_id}"' in raw_message
            or f"'qq': '{self_id}'" in raw_message
        ):
            return True
        return False

    async def _load_data(self) -> dict[str, Any]:
        data = await self.get_kv_data(self.DATA_KEY, {})
        if not isinstance(data, dict):
            data = {}

        users = data.get("users", {})
        settings = data.get("settings", {})
        chat_ts = data.get("chat_ts", {})
        redeems = data.get("redeems", [])
        redeem_seq = self._to_int(data.get("redeem_seq", 0), 0)
        speech_daily = data.get("speech_daily", {})
        speech_monthly = data.get("speech_monthly", {})
        if not isinstance(users, dict):
            users = {}
        if not isinstance(settings, dict):
            settings = {}
        if not isinstance(chat_ts, dict):
            chat_ts = {}
        if not isinstance(redeems, list):
            redeems = []
        if not isinstance(speech_daily, dict):
            speech_daily = {}
        if not isinstance(speech_monthly, dict):
            speech_monthly = {}

        return {
            "users": users,
            "settings": settings,
            "chat_ts": chat_ts,
            "redeems": redeems,
            "redeem_seq": max(0, redeem_seq),
            "speech_daily": speech_daily,
            "speech_monthly": speech_monthly,
        }

    async def _save_data(self, data: dict[str, Any]) -> None:
        await self.put_kv_data(self.DATA_KEY, data)

    def _ensure_user(self, data: dict[str, Any], user_id: str, user_name: str | None = None) -> dict[str, Any]:
        users = data["users"]
        user = users.get(user_id)
        if not isinstance(user, dict):
            user = {
                "points": 0,
                "last_sign_date": "",
                "name": user_name or user_id,
                "sign_streak": 0,
                "lottery_miss_streak": 0,
            }
            users[user_id] = user

        user["points"] = self._to_int(user.get("points", 0), 0)
        user["last_sign_date"] = str(user.get("last_sign_date", ""))
        user["sign_streak"] = max(0, self._to_int(user.get("sign_streak", 0), 0))
        user["lottery_miss_streak"] = max(0, self._to_int(user.get("lottery_miss_streak", 0), 0))
        if user_name:
            user["name"] = user_name
        elif "name" not in user:
            user["name"] = user_id
        return user

    @staticmethod
    def _change_points(user: dict[str, Any], delta: int) -> None:
        user["points"] = max(0, int(user.get("points", 0)) + delta)

    @staticmethod
    def _month_index(month_key: str) -> int | None:
        text = str(month_key or "").strip()
        if not re.match(r"^\d{4}-\d{2}$", text):
            return None
        year = int(text[:4])
        month = int(text[5:7])
        if month < 1 or month > 12:
            return None
        return year * 12 + month

    def _prune_speech_stats(self, data: dict[str, Any]) -> None:
        speech_daily = data.get("speech_daily")
        speech_monthly = data.get("speech_monthly")
        if not isinstance(speech_daily, dict):
            data["speech_daily"] = {}
            speech_daily = data["speech_daily"]
        if not isinstance(speech_monthly, dict):
            data["speech_monthly"] = {}
            speech_monthly = data["speech_monthly"]

        today = date.today()
        daily_cutoff = (today - timedelta(days=self.SPEECH_DAILY_RETENTION_DAYS)).isoformat()
        for day_key in list(speech_daily.keys()):
            valid = isinstance(day_key, str) and day_key >= daily_cutoff and isinstance(speech_daily.get(day_key), dict)
            if not valid:
                speech_daily.pop(day_key, None)

        current_month_idx = today.year * 12 + today.month
        month_cutoff = current_month_idx - self.SPEECH_MONTHLY_RETENTION_MONTHS
        for month_key in list(speech_monthly.keys()):
            idx = self._month_index(str(month_key))
            valid = idx is not None and idx >= month_cutoff and isinstance(speech_monthly.get(month_key), dict)
            if not valid:
                speech_monthly.pop(month_key, None)

    def _increment_speech_count(
        self,
        container: dict[str, Any],
        period_key: str,
        group_id: str,
        user_id: str,
        user_name: str,
    ) -> None:
        period_data = container.get(period_key)
        if not isinstance(period_data, dict):
            period_data = {}
            container[period_key] = period_data

        group_data = period_data.get(group_id)
        if not isinstance(group_data, dict):
            group_data = {}
            period_data[group_id] = group_data

        user_data = group_data.get(user_id)
        if not isinstance(user_data, dict):
            user_data = {"count": 0, "name": user_name or user_id}
            group_data[user_id] = user_data

        user_data["count"] = max(0, self._to_int(user_data.get("count", 0), 0)) + 1
        user_data["name"] = user_name or user_id

    def _record_group_speech_stat(self, data: dict[str, Any], event: AstrMessageEvent, sender_name: str) -> bool:
        group_id = self._get_event_group_id(event)
        if not group_id:
            return False
        if not self._is_group_allowed_by_policy(group_id):
            return False

        sender_id = str(event.get_sender_id() or "").strip()
        if not sender_id:
            return False

        self_id = str(event.get_self_id() or "").strip()
        if self_id and sender_id == self_id:
            return False

        self._prune_speech_stats(data)
        today_key = date.today().isoformat()
        month_key = today_key[:7]
        self._increment_speech_count(data["speech_daily"], today_key, group_id, sender_id, sender_name)
        self._increment_speech_count(data["speech_monthly"], month_key, group_id, sender_id, sender_name)
        return True

    def _flatten_speech_rows(self, period_data: Any) -> list[tuple[str, str, str, int]]:
        rows: list[tuple[str, str, str, int]] = []
        if not isinstance(period_data, dict):
            return rows

        for group_id, group_users in period_data.items():
            if not isinstance(group_users, dict):
                continue
            if not self._is_group_allowed_by_policy(str(group_id)):
                continue

            for user_id, user_data in group_users.items():
                uid = str(user_id)
                if isinstance(user_data, dict):
                    count = max(0, self._to_int(user_data.get("count", 0), 0))
                    name = str(user_data.get("name") or uid)
                else:
                    count = max(0, self._to_int(user_data, 0))
                    name = uid

                if count <= 0:
                    continue
                rows.append((str(group_id), name, uid, count))

        rows.sort(key=lambda x: (-x[3], x[0], x[2]))
        return rows

    def _get_reward_settings(self, data: dict[str, Any]) -> tuple[int, int, int]:
        settings = data["settings"]
        chance = self._to_int(settings.get("chance_percent"), self._cfg_int("random_reward_chance_percent", 5, 0, 100))
        reward_min = self._to_int(settings.get("reward_min"), self._cfg_int("random_reward_min", 1, 1))
        reward_max = self._to_int(settings.get("reward_max"), self._cfg_int("random_reward_max", 10, 1))

        chance = max(0, min(100, chance))
        reward_min = max(1, reward_min)
        reward_max = max(1, reward_max)
        if reward_min > reward_max:
            reward_min, reward_max = reward_max, reward_min
        return chance, reward_min, reward_max

    def _get_sign_in_lucky_settings(self, data: dict[str, Any]) -> tuple[int, int, int]:
        settings = data["settings"]
        chance = self._to_int(
            settings.get("sign_lucky_bonus_chance"),
            self._cfg_int("sign_in_lucky_bonus_chance_percent", 20, 0, 100),
        )
        bonus_min = self._to_int(
            settings.get("sign_lucky_bonus_min"),
            self._cfg_int("sign_in_lucky_bonus_min", 1, 1),
        )
        bonus_max = self._to_int(
            settings.get("sign_lucky_bonus_max"),
            self._cfg_int("sign_in_lucky_bonus_max", 10, 1),
        )

        chance = max(0, min(100, chance))
        bonus_min = max(1, bonus_min)
        bonus_max = max(1, bonus_max)
        if bonus_min > bonus_max:
            bonus_min, bonus_max = bonus_max, bonus_min
        return chance, bonus_min, bonus_max

    @staticmethod
    def _sanitize_product_name(value: Any, fallback: str) -> str:
        name = str(value or "").strip()
        if not name:
            return fallback
        return name[:40]

    def _get_redeem_products(self, data: dict[str, Any]) -> list[str]:
        settings = data["settings"]
        config_products = []
        if isinstance(self.config, dict):
            config_products = self.config.get("redeem_products", [])
        raw_products = settings.get("redeem_products", config_products)
        products: list[str] = []

        if isinstance(raw_products, list):
            for item in raw_products:
                name = str(item or "").strip()
                if name:
                    products.append(name[:40])
        else:
            text = str(raw_products or "")
            for item in re.split(r"[,，;；\n\r\t]+", text):
                name = item.strip()
                if name:
                    products.append(name[:40])

        slot_count = self._cfg_int(
            "dashboard_product_slot_count",
            self.DEFAULT_PRODUCT_SLOT_COUNT,
            1,
            self.DEFAULT_PRODUCT_SLOT_MAX,
        )
        slot_max = self._cfg_int(
            "dashboard_product_slots_max",
            self.DEFAULT_PRODUCT_SLOT_MAX,
            slot_count,
            100,
        )

        products = products[:slot_max]
        while len(products) < slot_count:
            products.append(f"商品{len(products) + 1}")
        return products

    def _set_redeem_products(self, data: dict[str, Any], products: list[str]) -> None:
        slot_max = self._cfg_int(
            "dashboard_product_slots_max",
            self.DEFAULT_PRODUCT_SLOT_MAX,
            1,
            100,
        )
        normalized: list[str] = []
        for idx, item in enumerate(products[:slot_max], start=1):
            normalized.append(self._sanitize_product_name(item, f"商品{idx}"))
        data["settings"]["redeem_products"] = normalized

    def _get_lottery_settings(self, data: dict[str, Any]) -> dict[str, Any]:
        settings = data["settings"]
        draw_cost = self._to_int(settings.get("lottery_cost"), self._cfg_int("lottery_cost_points", 10, 1))

        p1_points = self._to_int(
            settings.get("lottery_prize_1_points"),
            self._cfg_int("lottery_prize_1_points", 100, 1),
        )
        p2_points = self._to_int(
            settings.get("lottery_prize_2_points"),
            self._cfg_int("lottery_prize_2_points", 30, 1),
        )
        p3_points = self._to_int(
            settings.get("lottery_prize_3_points"),
            self._cfg_int("lottery_prize_3_points", 10, 1),
        )

        c1 = self._to_int(
            settings.get("lottery_prize_1_rate"),
            self._cfg_int("lottery_prize_1_chance_percent", 1, 0, 100),
        )
        c2 = self._to_int(
            settings.get("lottery_prize_2_rate"),
            self._cfg_int("lottery_prize_2_chance_percent", 5, 0, 100),
        )
        c3 = self._to_int(
            settings.get("lottery_prize_3_rate"),
            self._cfg_int("lottery_prize_3_chance_percent", 20, 0, 100),
        )

        c1 = max(0, min(100, c1))
        c2 = max(0, min(100, c2))
        c3 = max(0, min(100, c3))

        total = c1 + c2 + c3
        if total > 100:
            excess = total - 100
            for level in (3, 2, 1):
                if excess <= 0:
                    break
                if level == 3:
                    reduce_num = min(c3, excess)
                    c3 -= reduce_num
                    excess -= reduce_num
                elif level == 2:
                    reduce_num = min(c2, excess)
                    c2 -= reduce_num
                    excess -= reduce_num
                else:
                    reduce_num = min(c1, excess)
                    c1 -= reduce_num
                    excess -= reduce_num

        return {
            "cost": max(1, draw_cost),
            "prizes": [
                (1, max(1, p1_points), c1),
                (2, max(1, p2_points), c2),
                (3, max(1, p3_points), c3),
            ],
            "total_chance": c1 + c2 + c3,
        }

    def _get_redeem_settings(self, data: dict[str, Any]) -> tuple[int, str]:
        settings = data["settings"]
        redeem_cost = self._to_int(
            settings.get("redeem_cost"),
            self._cfg_int("redeem_cost_points", 100, 1),
        )
        raw_notify = str(settings.get("redeem_notify_qq", self._cfg_str("redeem_notify_qq", ""))).strip()
        notify_qq = self._normalize_qq(raw_notify)
        return max(1, redeem_cost), notify_qq

    def _get_lottery_pity_threshold(self, data: dict[str, Any]) -> int:
        settings = data["settings"]
        pity_threshold = self._to_int(
            settings.get("lottery_pity_threshold"),
            self._cfg_int("lottery_pity_threshold", 10, 0, 100000),
        )
        return max(0, pity_threshold)

    @staticmethod
    def _now_str() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def _find_redeem_order(self, data: dict[str, Any], order_id: int) -> dict[str, Any] | None:
        for item in data["redeems"]:
            if not isinstance(item, dict):
                continue
            if self._to_int(item.get("id", 0), 0) == order_id:
                return item
        return None

    def _get_dashboard_token(self) -> str:
        return self._cfg_str("dashboard_token", "").strip()

    def _is_dashboard_authorized(self, request, payload: dict[str, Any] | None = None) -> bool:
        token = self._get_dashboard_token()
        if not token:
            return True
        req_token = str(request.query.get("token", "")).strip()
        if not req_token and payload is not None:
            req_token = str(payload.get("token", "")).strip()
        return req_token == token

    def _dashboard_redirect(self, message: str = ""):
        params: list[tuple[str, str]] = []
        token = self._get_dashboard_token()
        if token:
            params.append(("token", token))
        if message:
            params.append(("msg", message))
        location = "/"
        if params:
            location = f"/?{urlencode(params)}"
        raise web.HTTPFound(location)

    async def _start_dashboard_server(self) -> None:
        if self._dashboard_runner is not None:
            return

        if not self._cfg_bool("dashboard_enabled", True):
            self._dashboard_url = ""
            self._dashboard_error = ""
            return

        if web is None:
            self._dashboard_error = "未安装 aiohttp，无法启动积分看板。"
            logger.warning(self._dashboard_error)
            return

        host = self._cfg_str("dashboard_host", "127.0.0.1").strip() or "127.0.0.1"
        port = self._cfg_int("dashboard_port", 16666, 1, 65535)

        app = web.Application()
        app.router.add_get("/", self._dashboard_index)
        app.router.add_get("/healthz", self._dashboard_health)
        app.router.add_post("/redeem/approve", self._dashboard_redeem_approve)
        app.router.add_post("/products/update", self._dashboard_product_update)
        app.router.add_post("/products/add", self._dashboard_product_add)

        runner = web.AppRunner(app)
        try:
            await runner.setup()
            site = web.TCPSite(runner, host=host, port=port)
            await site.start()
            self._dashboard_runner = runner
            self._dashboard_url = f"http://{host}:{port}"
            self._dashboard_error = ""
            logger.info(f"积分看板已启动: {self._dashboard_url}")
        except Exception as exc:
            self._dashboard_error = f"启动积分看板失败: {exc}"
            self._dashboard_url = ""
            logger.warning(self._dashboard_error)
            try:
                await runner.cleanup()
            except Exception:
                pass

    async def _stop_dashboard_server(self) -> None:
        if self._dashboard_runner is None:
            return
        try:
            await self._dashboard_runner.cleanup()
        except Exception as exc:
            logger.warning(f"关闭积分看板失败: {exc}")
        finally:
            self._dashboard_runner = None
            self._dashboard_url = ""

    async def _dashboard_health(self, request):
        return web.json_response({"ok": True, "name": "astrbot_plugin_fun_dashboard"})

    async def _dashboard_redeem_approve(self, request):
        post_data = await request.post()
        if not self._is_dashboard_authorized(request, post_data):
            return web.Response(status=401, text="Unauthorized")

        order_id = self._to_int(post_data.get("order_id", 0), 0)
        if order_id <= 0:
            self._dashboard_redirect("无效兑换单号")

        data = await self._load_data()
        order = self._find_redeem_order(data, order_id)
        if not order:
            self._dashboard_redirect(f"未找到兑换单 #{order_id}")

        status = str(order.get("status", ""))
        if status == "已完成":
            self._dashboard_redirect(f"兑换单 #{order_id} 已完成")
        if status == "已处理":
            self._dashboard_redirect(f"兑换单 #{order_id} 已是已处理")

        order["status"] = "已处理"
        order["updated_at"] = self._now_str()
        order["handler_id"] = "dashboard"
        order["handler_name"] = "看板同意兑换"
        if not str(order.get("note", "")).strip():
            order["note"] = "看板同意兑换"

        await self._save_data(data)
        self._dashboard_redirect(f"兑换单 #{order_id} 已同意")

    async def _dashboard_product_update(self, request):
        post_data = await request.post()
        if not self._is_dashboard_authorized(request, post_data):
            return web.Response(status=401, text="Unauthorized")

        slot = self._to_int(post_data.get("slot", 0), 0)
        if slot <= 0:
            self._dashboard_redirect("无效商品位")

        data = await self._load_data()
        products = self._get_redeem_products(data)
        if slot > len(products):
            self._dashboard_redirect("商品位不存在")

        name = self._sanitize_product_name(post_data.get("name", ""), f"商品{slot}")
        products[slot - 1] = name
        self._set_redeem_products(data, products)
        await self._save_data(data)
        self._dashboard_redirect(f"商品位 #{slot} 已更新")

    async def _dashboard_product_add(self, request):
        post_data = await request.post()
        if not self._is_dashboard_authorized(request, post_data):
            return web.Response(status=401, text="Unauthorized")

        data = await self._load_data()
        products = self._get_redeem_products(data)
        slot_max = self._cfg_int(
            "dashboard_product_slots_max",
            self.DEFAULT_PRODUCT_SLOT_MAX,
            1,
            100,
        )

        if len(products) >= slot_max:
            self._dashboard_redirect(f"商品位已达上限（{slot_max}）")

        new_slot = len(products) + 1
        name = self._sanitize_product_name(post_data.get("name", ""), f"商品{new_slot}")
        products.append(name)
        self._set_redeem_products(data, products)
        await self._save_data(data)
        self._dashboard_redirect(f"已新增商品位 #{new_slot}")

    async def _dashboard_index(self, request):
        if not self._is_dashboard_authorized(request):
            return web.Response(status=401, text="Unauthorized")

        data = await self._load_data()
        flash_msg = str(request.query.get("msg", "")).strip()
        html_text = self._build_dashboard_html(data, flash_msg)
        return web.Response(text=html_text, content_type="text/html")

    def _build_dashboard_html(self, data: dict[str, Any], flash_msg: str = "") -> str:
        title = escape(self._cfg_str("dashboard_title", "积分看板"))
        refresh_seconds = self._cfg_int("dashboard_auto_refresh_seconds", 15, 0, 3600)
        dashboard_token = self._get_dashboard_token()
        products = self._get_redeem_products(data)
        product_slot_max = self._cfg_int(
            "dashboard_product_slots_max",
            self.DEFAULT_PRODUCT_SLOT_MAX,
            1,
            100,
        )
        refresh_meta = ""
        if refresh_seconds > 0:
            refresh_meta = f'<meta http-equiv="refresh" content="{refresh_seconds}">'

        today_key = date.today().isoformat()
        month_key = today_key[:7]
        speech_daily = data.get("speech_daily", {})
        speech_monthly = data.get("speech_monthly", {})
        daily_rows_data = self._flatten_speech_rows(
            speech_daily.get(today_key, {}) if isinstance(speech_daily, dict) else {}
        )
        monthly_rows_data = self._flatten_speech_rows(
            speech_monthly.get(month_key, {}) if isinstance(speech_monthly, dict) else {}
        )
        daily_total = sum(row[3] for row in daily_rows_data)
        monthly_total = sum(row[3] for row in monthly_rows_data)

        users: list[tuple[str, str, int, int]] = []
        for uid, item in data["users"].items():
            if not isinstance(item, dict):
                continue
            users.append(
                (
                    str(uid),
                    str(item.get("name") or uid),
                    self._to_int(item.get("points", 0), 0),
                    self._to_int(item.get("sign_streak", 0), 0),
                )
            )
        users.sort(key=lambda x: (-x[2], x[0]))

        points_rows = []
        for idx, (uid, name, points, streak) in enumerate(users, start=1):
            points_rows.append(
                "<tr>"
                f"<td>{idx}</td><td>{escape(name)}</td><td>{escape(uid)}</td><td>{points}</td><td>{streak}</td>"
                "</tr>"
            )
        if not points_rows:
            points_rows.append('<tr><td colspan="5">暂无积分数据</td></tr>')

        speech_daily_rows = []
        for idx, (group_id, name, uid, count) in enumerate(daily_rows_data[:200], start=1):
            speech_daily_rows.append(
                "<tr>"
                f"<td>{idx}</td><td>{escape(group_id)}</td><td>{escape(name)}</td><td>{escape(uid)}</td><td>{count}</td>"
                "</tr>"
            )
        if not speech_daily_rows:
            speech_daily_rows.append('<tr><td colspan="5">今日暂无发言数据</td></tr>')

        speech_monthly_rows = []
        for idx, (group_id, name, uid, count) in enumerate(monthly_rows_data[:200], start=1):
            speech_monthly_rows.append(
                "<tr>"
                f"<td>{idx}</td><td>{escape(group_id)}</td><td>{escape(name)}</td><td>{escape(uid)}</td><td>{count}</td>"
                "</tr>"
            )
        if not speech_monthly_rows:
            speech_monthly_rows.append('<tr><td colspan="5">本月暂无发言数据</td></tr>')

        status_counts = {"已申请": 0, "已处理": 0, "已完成": 0}
        records: list[dict[str, Any]] = []
        for item in data["redeems"]:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status", ""))
            if status in status_counts:
                status_counts[status] += 1
            records.append(item)
        records.sort(key=lambda x: self._to_int(x.get("id", 0), 0), reverse=True)

        redeem_rows = []
        for item in records:
            order_id = self._to_int(item.get("id", 0), 0)
            status_raw = str(item.get("status", "未知"))
            status = escape(status_raw)
            user_name = escape(str(item.get("user_name", "")))
            user_id = escape(str(item.get("user_id", "")))
            cost = self._to_int(item.get("cost", 0), 0)
            reason = escape(str(item.get("reason", "")).replace("\n", " "))
            updated_at = escape(str(item.get("updated_at", "")))
            handler_name = escape(str(item.get("handler_name", "")))

            action_html = "-"
            if status_raw == "已申请":
                token_input = ""
                if dashboard_token:
                    token_input = (
                        f'<input type="hidden" name="token" value="{escape(dashboard_token)}">'
                    )
                action_html = (
                    '<form method="post" action="/redeem/approve" style="margin:0;">'
                    f'<input type="hidden" name="order_id" value="{order_id}">'
                    f"{token_input}"
                    '<button type="submit">同意兑换</button>'
                    "</form>"
                )

            redeem_rows.append(
                "<tr>"
                f"<td>{order_id}</td><td>{status}</td><td>{user_name}</td><td>{user_id}</td>"
                f"<td>{cost}</td><td>{reason}</td><td>{updated_at}</td><td>{handler_name}</td><td>{action_html}</td>"
                "</tr>"
            )
        if not redeem_rows:
            redeem_rows.append('<tr><td colspan="9">暂无兑换记录</td></tr>')

        flash_html = ""
        if flash_msg:
            flash_html = f'<div class="meta notice">操作结果：{escape(flash_msg)}</div>'

        product_rows = []
        token_input = ""
        if dashboard_token:
            token_input = f'<input type="hidden" name="token" value="{escape(dashboard_token)}">'

        for idx, name in enumerate(products, start=1):
            product_rows.append(
                "<tr>"
                f"<td>{idx}</td>"
                f"<td>{escape(name)}</td>"
                "<td>"
                '<form method="post" action="/products/update" class="inline-form">'
                f"{token_input}"
                f'<input type="hidden" name="slot" value="{idx}">'
                f'<input type="text" name="name" value="{escape(name)}" maxlength="40">'
                '<button type="submit">保存</button>'
                "</form>"
                "</td>"
                "</tr>"
            )
        if not product_rows:
            product_rows.append('<tr><td colspan="3">暂无商品位</td></tr>')

        add_product_form = ""
        if len(products) < product_slot_max:
            add_product_form = (
                '<form method="post" action="/products/add" class="inline-form">'
                f"{token_input}"
                '<input type="text" name="name" placeholder="新商品名（可留空自动命名）" maxlength="40">'
                '<button type="submit">新增商品位</button>'
                "</form>"
            )
        else:
            add_product_form = f'<div class="meta">商品位已达到上限：{product_slot_max}</div>'

        return (
            "<!doctype html><html><head><meta charset=\"utf-8\">"
            f"{refresh_meta}"
            f"<title>{title}</title>"
            "<style>"
            "body{font-family:Segoe UI,Microsoft YaHei,sans-serif;background:#f5f7fb;color:#1f2937;padding:20px;}"
            ".card{background:#fff;border-radius:12px;padding:16px;margin-bottom:16px;box-shadow:0 4px 14px rgba(0,0,0,.05);}"
            "h1{margin:0 0 10px;font-size:28px;}"
            "h2{margin:8px 0 12px;font-size:20px;}"
            "table{width:100%;border-collapse:collapse;font-size:14px;}"
            "th,td{border-bottom:1px solid #e5e7eb;padding:8px;text-align:left;vertical-align:top;}"
            "th{background:#f3f4f6;}"
            ".meta{color:#4b5563;font-size:13px;margin:8px 0;}"
            ".notice{color:#0f766e;font-weight:600;}"
            ".inline-form{display:flex;gap:8px;align-items:center;flex-wrap:wrap;}"
            "input[type=text]{min-width:220px;padding:6px 8px;border:1px solid #d1d5db;border-radius:8px;}"
            "button{border:0;border-radius:8px;padding:6px 10px;background:#1d4ed8;color:#fff;cursor:pointer;}"
            "button:hover{background:#1e40af;}"
            "</style></head><body>"
            f"<div class=\"card\"><h1>{title}</h1>"
            f"<div class=\"meta\">更新时间：{escape(self._now_str())} | 用户数：{len(users)} | 兑换单：{len(records)}</div>"
            f"<div class=\"meta\">状态统计：已申请 {status_counts['已申请']}，已处理 {status_counts['已处理']}，已完成 {status_counts['已完成']}</div>"
            f"{flash_html}"
            "</div>"
            "<div class=\"card\"><h2>每个人的积分</h2>"
            "<table><thead><tr><th>排名</th><th>昵称</th><th>QQ</th><th>积分</th><th>连签</th></tr></thead>"
            f"<tbody>{''.join(points_rows)}</tbody></table></div>"
            f"<div class=\"card\"><h2>群成员每日发言统计（{escape(today_key)}）</h2>"
            f"<div class=\"meta\">当日总发言：{daily_total} 条，展示前 200 名</div>"
            "<table><thead><tr><th>排名</th><th>群号</th><th>昵称</th><th>QQ</th><th>发言次数</th></tr></thead>"
            f"<tbody>{''.join(speech_daily_rows)}</tbody></table></div>"
            f"<div class=\"card\"><h2>群成员每月发言统计（{escape(month_key)}）</h2>"
            f"<div class=\"meta\">当月总发言：{monthly_total} 条，展示前 200 名</div>"
            "<table><thead><tr><th>排名</th><th>群号</th><th>昵称</th><th>QQ</th><th>发言次数</th></tr></thead>"
            f"<tbody>{''.join(speech_monthly_rows)}</tbody></table></div>"
            "<div class=\"card\"><h2>兑换商品位（可自定义名称）</h2>"
            f"<div class=\"meta\">当前商品位：{len(products)} / {product_slot_max}</div>"
            "<table><thead><tr><th>商品位</th><th>商品名称</th><th>编辑</th></tr></thead>"
            f"<tbody>{''.join(product_rows)}</tbody></table>"
            f"{add_product_form}</div>"
            "<div class=\"card\"><h2>兑换申请与记录</h2>"
            "<table><thead><tr><th>单号</th><th>状态</th><th>申请人</th><th>QQ</th><th>积分</th><th>说明</th><th>更新时间</th><th>处理人</th><th>操作</th></tr></thead>"
            f"<tbody>{''.join(redeem_rows)}</tbody></table></div>"
            "</body></html>"
        )

    @staticmethod
    def _normalize_qq(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if text.isdigit():
            return text
        match = re.search(r"\d{5,}", text)
        if match:
            return match.group(0)
        return ""

    @staticmethod
    def _build_notify_chain(notify_qq: str, notify_text: str) -> list[Any]:
        return [
            Comp.At(qq=notify_qq),
            Comp.Plain(f" @{notify_qq} "),
            Comp.Plain(notify_text),
        ]

    async def _notify_private_qq(self, event: AstrMessageEvent, notify_qq: str, notify_text: str) -> tuple[bool, str]:
        platform_id = str(event.get_platform_id() or "").strip()
        if not platform_id:
            return False, "无法识别当前平台ID"

        session = f"{platform_id}:FriendMessage:{notify_qq}"
        try:
            sent = await self.context.send_message(
                session,
                MessageChain([Comp.Plain(notify_text)]),
            )
            if sent:
                return True, ""
            return False, "未找到可发送的平台会话"
        except Exception as exc:
            logger.warning(f"发送私聊兑换通知失败: {exc}")
            return False, str(exc)

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def random_chat_reward(self, event: AstrMessageEvent):
        """聊天概率触发随机积分奖励。"""
        if not self._is_event_allowed(event):
            return

        message_str = (event.message_str or "").strip()
        if not message_str and not self._extract_message_components(event):
            return

        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name() or sender_id
        self_id = str(event.get_self_id() or "").strip()
        if self_id and sender_id == self_id:
            return

        data: dict[str, Any] | None = None
        user: dict[str, Any] | None = None
        speech_changed = False
        group_id = self._get_event_group_id(event)

        if group_id:
            data = await self._load_data()
            user = self._ensure_user(data, sender_id, sender_name)
            speech_changed = self._record_group_speech_stat(data, event, sender_name)

            allow_member_chat_reward = self._cfg_bool("group_chat_reward_for_members", True)
            if not allow_member_chat_reward and not self._is_message_to_bot(event):
                if speech_changed:
                    await self._save_data(data)
                return

        if message_str.startswith("/"):
            if speech_changed and data is not None:
                await self._save_data(data)
            return

        if data is None or user is None:
            data = await self._load_data()
            user = self._ensure_user(data, sender_id, sender_name)
        cooldown = self._cfg_int("chat_reward_cooldown_seconds", 30, 0, 86400)

        now = time.time()
        last_ts = float(data["chat_ts"].get(sender_id, 0))
        if cooldown > 0 and now - last_ts < cooldown:
            return
        data["chat_ts"][sender_id] = now

        chance, reward_min, reward_max = self._get_reward_settings(data)
        hit = False
        if chance >= 100:
            hit = True
        elif chance > 0:
            hit = random.randint(1, 100) <= chance

        if not hit:
            await self._save_data(data)
            return

        gained = random.randint(reward_min, reward_max)
        self._change_points(user, gained)
        await self._save_data(data)
        yield event.plain_result(
            f"随机奖励触发，{sender_name} 获得 {gained} 积分，当前积分 {user['points']}。"
        )

    @filter.command("签到")
    async def sign_in(self, event: AstrMessageEvent):
        """每日签到领取基础积分。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name() or sender_id
        today = date.today().isoformat()
        tomorrow = (date.today() + timedelta(days=1)).isoformat()

        data = await self._load_data()
        user = self._ensure_user(data, sender_id, sender_name)
        if user.get("last_sign_date") == today:
            yield event.plain_result(
                f"{sender_name} 今天已经签过到了（每日仅可签到一次），当前积分 {user['points']}。\n"
                f"请在 {tomorrow} 再来签到。"
            )
            return

        streak = 1
        last_sign_date = str(user.get("last_sign_date", ""))
        if last_sign_date:
            try:
                last_date = date.fromisoformat(last_sign_date)
                if last_date == date.today() - timedelta(days=1):
                    streak = user.get("sign_streak", 0) + 1
            except ValueError:
                streak = 1

        base_points = self._cfg_int("sign_in_points", 8, 1, 10000)
        streak_bonus_per_day = self._cfg_int("sign_in_streak_bonus_per_day", 2, 0, 10000)
        streak_bonus_max = self._cfg_int("sign_in_streak_bonus_max", 30, 0, 100000)
        lucky_chance, lucky_min, lucky_max = self._get_sign_in_lucky_settings(data)

        extra_bonus = max(0, streak - 1) * streak_bonus_per_day
        extra_bonus = min(extra_bonus, streak_bonus_max)
        lucky_bonus = 0
        if lucky_chance >= 100 or (lucky_chance > 0 and random.randint(1, 100) <= lucky_chance):
            lucky_bonus = random.randint(lucky_min, lucky_max)
        sign_points = base_points + extra_bonus + lucky_bonus

        self._change_points(user, sign_points)
        user["last_sign_date"] = today
        user["sign_streak"] = streak
        await self._save_data(data)

        lucky_text = ""
        if lucky_bonus > 0:
            lucky_text = f"，签到幸运加成 {lucky_bonus}"

        yield event.plain_result(
            f"签到成功，{sender_name} 连续签到 {streak} 天，"
            f"基础 {base_points} + 连签加成 {extra_bonus}{lucky_text} = {sign_points} 积分，当前积分 {user['points']}。"
        )

    @filter.command("查询", alias={"余额", "我的积分"})
    async def query_points(self, event: AstrMessageEvent, qq: str = ""):
        """查询自己或指定 QQ 的积分。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name() or sender_id
        target_id = qq.strip() or sender_id

        data = await self._load_data()
        if target_id == sender_id:
            user = self._ensure_user(data, target_id, sender_name)
        else:
            user = self._ensure_user(data, target_id)

        await self._save_data(data)
        target_name = user.get("name") or target_id
        streak = self._to_int(user.get("sign_streak", 0), 0)
        yield event.plain_result(f"{target_name}({target_id}) 当前积分：{user['points']}，连续签到：{streak} 天")

    @filter.command("排行", alias={"排行榜"})
    async def rank_points(self, event: AstrMessageEvent, top_n: int = 0):
        """查看积分排行榜。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if top_n <= 0:
            top_n = self._cfg_int("leaderboard_default_size", 10, 1, 50)
        top_n = min(top_n, 50)

        data = await self._load_data()
        users = data["users"]
        if not users:
            yield event.plain_result("当前还没有积分数据。")
            return

        ranking: list[tuple[str, str, int]] = []
        for uid, user in users.items():
            if not isinstance(user, dict):
                continue
            points = self._to_int(user.get("points", 0), 0)
            name = str(user.get("name") or uid)
            ranking.append((uid, name, points))

        ranking.sort(key=lambda x: (-x[2], x[0]))
        lines = ["积分排行榜："]
        for idx, (uid, name, points) in enumerate(ranking[:top_n], start=1):
            lines.append(f"{idx}. {name}({uid}) - {points}")
        yield event.plain_result("\n".join(lines))

    @filter.command("抽奖")
    async def lottery_draw(self, event: AstrMessageEvent, times: int = 1):
        """消耗积分进行抽奖，支持一等奖/二等奖/三等奖。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if times <= 0:
            yield event.plain_result("抽奖次数必须大于 0。")
            return
        if times > 20:
            yield event.plain_result("单次最多抽奖 20 次。")
            return

        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name() or sender_id
        data = await self._load_data()
        sender_user = self._ensure_user(data, sender_id, sender_name)

        lottery = self._get_lottery_settings(data)
        cost_per_draw = lottery["cost"]
        total_cost = cost_per_draw * times
        if sender_user["points"] < total_cost:
            yield event.plain_result(
                f"积分不足，抽奖 {times} 次需要 {total_cost} 积分（每次 {cost_per_draw}），当前 {sender_user['points']}。"
            )
            return

        self._change_points(sender_user, -total_cost)

        counts = {1: 0, 2: 0, 3: 0}
        reward_total = 0
        pity_hit_count = 0
        lottery_miss_streak = self._to_int(sender_user.get("lottery_miss_streak", 0), 0)
        pity_threshold = self._get_lottery_pity_threshold(data)
        prize_points = {level: reward_points for level, reward_points, _ in lottery["prizes"]}
        for _ in range(times):
            if pity_threshold > 0 and lottery_miss_streak + 1 >= pity_threshold:
                counts[3] += 1
                reward_total += prize_points.get(3, 0)
                lottery_miss_streak = 0
                pity_hit_count += 1
                continue

            roll = random.randint(1, 100)
            cumulative = 0
            won = False
            for level, reward_points, chance in lottery["prizes"]:
                cumulative += chance
                if roll <= cumulative and chance > 0:
                    counts[level] += 1
                    reward_total += reward_points
                    won = True
                    break

            if won:
                lottery_miss_streak = 0
            else:
                lottery_miss_streak += 1

        sender_user["lottery_miss_streak"] = lottery_miss_streak

        self._change_points(sender_user, reward_total)
        await self._save_data(data)

        pity_text = "保底已关闭"
        if pity_threshold > 0:
            pity_text = f"当前未中奖连抽 {lottery_miss_streak}/{pity_threshold}"

        yield event.plain_result(
            f"{sender_name} 抽奖完成：共 {times} 次，消耗 {total_cost} 积分，获得奖励 {reward_total} 积分。\n"
            f"一等奖 {counts[1]} 次，二等奖 {counts[2]} 次，三等奖 {counts[3]} 次。\n"
            f"保底触发 {pity_hit_count} 次，{pity_text}。\n"
            f"当前积分 {sender_user['points']}。"
        )

    @filter.command("兑换")
    async def redeem_points(self, event: AstrMessageEvent, reason: str = "请处理兑换申请"):
        """达到兑换门槛后，创建兑换申请并提醒指定 QQ。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name() or sender_id
        data = await self._load_data()
        sender_user = self._ensure_user(data, sender_id, sender_name)

        redeem_cost, notify_qq = self._get_redeem_settings(data)

        if sender_user["points"] < redeem_cost:
            yield event.plain_result(f"积分不足，当前 {sender_user['points']}，兑换需要 {redeem_cost}。")
            return

        self._change_points(sender_user, -redeem_cost)

        order_id = data["redeem_seq"] + 1
        data["redeem_seq"] = order_id
        order = {
            "id": order_id,
            "status": "已申请",
            "user_id": sender_id,
            "user_name": sender_name,
            "cost": redeem_cost,
            "reason": reason,
            "created_at": self._now_str(),
            "updated_at": self._now_str(),
            "handler_id": "",
            "handler_name": "",
            "note": "",
        }
        data["redeems"].append(order)
        if len(data["redeems"]) > 1000:
            data["redeems"] = data["redeems"][-1000:]

        await self._save_data(data)

        notify_text = (
            f"兑换申请 #{order_id}（状态：已申请）：{sender_name}({sender_id})"
            f" 使用 {redeem_cost} 积分，说明：{reason}。剩余积分 {sender_user['points']}。"
        )

        if notify_qq:
            if self._get_event_group_id(event):
                yield event.chain_result(self._build_notify_chain(notify_qq, notify_text))
            else:
                yield event.plain_result(notify_text)

            private_sent, private_err = await self._notify_private_qq(event, notify_qq, notify_text)
            if private_sent:
                yield event.plain_result(f"已私聊通知 {notify_qq}。")
            else:
                yield event.plain_result(f"群内提醒已发送，但私聊通知失败：{private_err}")
            return

        yield event.plain_result(f"兑换申请已创建，但未识别到有效通知QQ。{notify_text}")

    @filter.command("兑换状态")
    async def redeem_status(self, event: AstrMessageEvent, order_id: int = 0):
        """查询兑换审核状态。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        sender_id = str(event.get_sender_id())
        data = await self._load_data()

        if order_id > 0:
            order = self._find_redeem_order(data, order_id)
            if not order:
                yield event.plain_result(f"未找到兑换单 #{order_id}。")
                return

            yield event.plain_result(
                f"兑换单 #{order['id']}\n"
                f"- 申请人：{order.get('user_name', '')}({order.get('user_id', '')})\n"
                f"- 状态：{order.get('status', '未知')}\n"
                f"- 积分：{order.get('cost', 0)}\n"
                f"- 说明：{order.get('reason', '')}\n"
                f"- 创建时间：{order.get('created_at', '')}\n"
                f"- 更新时间：{order.get('updated_at', '')}\n"
                f"- 处理人：{order.get('handler_name', '')}({order.get('handler_id', '')})\n"
                f"- 备注：{order.get('note', '') or '无'}"
            )
            return

        own_orders = []
        for item in data["redeems"]:
            if not isinstance(item, dict):
                continue
            if str(item.get("user_id", "")) == sender_id:
                own_orders.append(item)

        if not own_orders:
            yield event.plain_result("你还没有兑换记录。")
            return

        own_orders = own_orders[-5:]
        lines = ["你最近的兑换记录："]
        for item in own_orders:
            lines.append(
                f"#{item.get('id', 0)} - {item.get('status', '未知')} - {item.get('cost', 0)}积分 - {item.get('updated_at', '')}"
            )
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("群控状态")
    async def admin_group_control_status(self, event: AstrMessageEvent):
        """管理员查看群白名单/黑名单和当前群启用状态。"""
        group_id = self._get_event_group_id(event)
        whitelist = self._cfg_group_id_list("group_whitelist")
        blacklist = self._cfg_group_id_list("group_blacklist")
        private_enabled = self._cfg_bool("group_enable_private", True)
        current_enabled = self._is_event_allowed(event)

        lines = ["群控配置："]
        if group_id:
            lines.append(f"- 当前群：{group_id}")
        else:
            lines.append("- 当前会话：私聊")
        lines.append(f"- 当前会话是否启用：{'是' if current_enabled else '否'}")
        lines.append(f"- 私聊是否启用：{'是' if private_enabled else '否'}")
        lines.append(f"- 白名单数量：{len(whitelist)}")
        lines.append(f"- 黑名单数量：{len(blacklist)}")
        if whitelist:
            lines.append(f"- 白名单：{', '.join(whitelist[:20])}")
        if blacklist:
            lines.append(f"- 黑名单：{', '.join(blacklist[:20])}")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("白名单加")
    async def admin_group_whitelist_add(self, event: AstrMessageEvent, group_ids: str = ""):
        """管理员添加一个或多个群到白名单（逗号分隔）。"""
        targets = self._parse_id_text(group_ids, self._normalize_group_id)
        if not targets:
            target = self._resolve_target_group(event, "")
            if target:
                targets = [target]
        if not targets:
            yield event.plain_result("请提供群号（支持逗号分隔多个），或在群聊中直接执行。")
            return

        whitelist = self._cfg_group_id_list("group_whitelist")
        added: list[str] = []
        existed: list[str] = []
        for target in targets:
            if target in whitelist:
                existed.append(target)
                continue
            whitelist.append(target)
            added.append(target)

        if not added:
            yield event.plain_result(f"目标群已全部在白名单中：{', '.join(existed)}")
            return

        self._set_group_id_list("group_whitelist", whitelist)
        msg = f"已加入白名单：{', '.join(added)}"
        if existed:
            msg += f"\n已存在：{', '.join(existed)}"
        yield event.plain_result(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("白名单删")
    async def admin_group_whitelist_remove(self, event: AstrMessageEvent, group_ids: str = ""):
        """管理员从白名单移除一个或多个群（逗号分隔）。"""
        targets = self._parse_id_text(group_ids, self._normalize_group_id)
        if not targets:
            target = self._resolve_target_group(event, "")
            if target:
                targets = [target]
        if not targets:
            yield event.plain_result("请提供有效群号（支持逗号分隔多个），或在群聊中直接执行。")
            return

        whitelist = self._cfg_group_id_list("group_whitelist")
        removed: list[str] = []
        missing: list[str] = []
        for target in targets:
            if target in whitelist:
                removed.append(target)
            else:
                missing.append(target)

        if not removed:
            yield event.plain_result(f"目标群均不在白名单中：{', '.join(missing)}")
            return

        whitelist = [gid for gid in whitelist if gid not in set(removed)]
        self._set_group_id_list("group_whitelist", whitelist)
        msg = f"已从白名单移除：{', '.join(removed)}"
        if missing:
            msg += f"\n原本不存在：{', '.join(missing)}"
        yield event.plain_result(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("白名单列表")
    async def admin_group_whitelist_list(self, event: AstrMessageEvent):
        """管理员查看白名单群列表。"""
        whitelist = self._cfg_group_id_list("group_whitelist")
        if not whitelist:
            yield event.plain_result("白名单为空。")
            return
        yield event.plain_result("白名单群：\n" + "\n".join(whitelist))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("黑名单加")
    async def admin_group_blacklist_add(self, event: AstrMessageEvent, group_ids: str = ""):
        """管理员添加一个或多个群到黑名单（逗号分隔）。"""
        targets = self._parse_id_text(group_ids, self._normalize_group_id)
        if not targets:
            target = self._resolve_target_group(event, "")
            if target:
                targets = [target]
        if not targets:
            yield event.plain_result("请提供群号（支持逗号分隔多个），或在群聊中直接执行。")
            return

        blacklist = self._cfg_group_id_list("group_blacklist")
        added: list[str] = []
        existed: list[str] = []
        for target in targets:
            if target in blacklist:
                existed.append(target)
                continue
            blacklist.append(target)
            added.append(target)

        if not added:
            yield event.plain_result(f"目标群已全部在黑名单中：{', '.join(existed)}")
            return

        self._set_group_id_list("group_blacklist", blacklist)
        msg = f"已加入黑名单：{', '.join(added)}"
        if existed:
            msg += f"\n已存在：{', '.join(existed)}"
        yield event.plain_result(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("黑名单删")
    async def admin_group_blacklist_remove(self, event: AstrMessageEvent, group_ids: str = ""):
        """管理员从黑名单移除一个或多个群（逗号分隔）。"""
        targets = self._parse_id_text(group_ids, self._normalize_group_id)
        if not targets:
            target = self._resolve_target_group(event, "")
            if target:
                targets = [target]
        if not targets:
            yield event.plain_result("请提供有效群号（支持逗号分隔多个），或在群聊中直接执行。")
            return

        blacklist = self._cfg_group_id_list("group_blacklist")
        removed: list[str] = []
        missing: list[str] = []
        for target in targets:
            if target in blacklist:
                removed.append(target)
            else:
                missing.append(target)

        if not removed:
            yield event.plain_result(f"目标群均不在黑名单中：{', '.join(missing)}")
            return

        blacklist = [gid for gid in blacklist if gid not in set(removed)]
        self._set_group_id_list("group_blacklist", blacklist)
        msg = f"已从黑名单移除：{', '.join(removed)}"
        if missing:
            msg += f"\n原本不存在：{', '.join(missing)}"
        yield event.plain_result(msg)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("黑名单列表")
    async def admin_group_blacklist_list(self, event: AstrMessageEvent):
        """管理员查看黑名单群列表。"""
        blacklist = self._cfg_group_id_list("group_blacklist")
        if not blacklist:
            yield event.plain_result("黑名单为空。")
            return
        yield event.plain_result("黑名单群：\n" + "\n".join(blacklist))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("加分")
    async def admin_add_points(self, event: AstrMessageEvent, qq: str, points: int):
        """管理员给指定 QQ 增加积分。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if points <= 0:
            yield event.plain_result("加分数值必须大于 0。")
            return

        data = await self._load_data()
        user = self._ensure_user(data, qq)
        self._change_points(user, points)
        await self._save_data(data)
        yield event.plain_result(f"已为 {qq} 增加 {points} 积分，当前 {user['points']}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("扣分")
    async def admin_sub_points(self, event: AstrMessageEvent, qq: str, points: int):
        """管理员扣除指定 QQ 的积分。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if points <= 0:
            yield event.plain_result("扣分数值必须大于 0。")
            return

        data = await self._load_data()
        user = self._ensure_user(data, qq)
        self._change_points(user, -points)
        await self._save_data(data)
        yield event.plain_result(f"已扣除 {qq} {points} 积分，当前 {user['points']}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置概率")
    async def admin_set_chance(self, event: AstrMessageEvent, chance_percent: int):
        """管理员设置聊天随机奖励概率。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if chance_percent < 0 or chance_percent > 100:
            yield event.plain_result("概率范围必须在 0 到 100 之间。")
            return

        data = await self._load_data()
        data["settings"]["chance_percent"] = chance_percent
        await self._save_data(data)
        yield event.plain_result(f"随机奖励概率已设置为 {chance_percent}%。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置范围")
    async def admin_set_range(self, event: AstrMessageEvent, reward_min: int, reward_max: int):
        """管理员设置聊天随机奖励积分区间。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if reward_min <= 0 or reward_max <= 0:
            yield event.plain_result("积分范围必须是正整数。")
            return

        if reward_min > reward_max:
            reward_min, reward_max = reward_max, reward_min

        data = await self._load_data()
        data["settings"]["reward_min"] = reward_min
        data["settings"]["reward_max"] = reward_max
        await self._save_data(data)
        yield event.plain_result(f"随机奖励范围已设置为 {reward_min}-{reward_max}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置奖项")
    async def admin_set_lottery_prize(self, event: AstrMessageEvent, level: int, points: int, chance_percent: int):
        """管理员设置抽奖奖项积分与概率。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if level not in (1, 2, 3):
            yield event.plain_result("奖项等级只能是 1、2、3。")
            return
        if points <= 0:
            yield event.plain_result("奖项积分必须大于 0。")
            return
        if chance_percent < 0 or chance_percent > 100:
            yield event.plain_result("奖项概率必须在 0 到 100 之间。")
            return

        data = await self._load_data()
        lottery = self._get_lottery_settings(data)
        chances = {
            1: lottery["prizes"][0][2],
            2: lottery["prizes"][1][2],
            3: lottery["prizes"][2][2],
        }
        chances[level] = chance_percent
        if sum(chances.values()) > 100:
            yield event.plain_result("三个奖项概率总和不能超过 100。")
            return

        data["settings"][f"lottery_prize_{level}_points"] = points
        data["settings"][f"lottery_prize_{level}_rate"] = chance_percent
        await self._save_data(data)
        yield event.plain_result(f"抽奖奖项已更新：{level} 等奖 {points} 积分，概率 {chance_percent}%。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置抽奖消耗")
    async def admin_set_lottery_cost(self, event: AstrMessageEvent, cost_points: int):
        """管理员设置每次抽奖消耗积分。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if cost_points <= 0:
            yield event.plain_result("抽奖消耗必须大于 0。")
            return

        data = await self._load_data()
        data["settings"]["lottery_cost"] = cost_points
        await self._save_data(data)
        yield event.plain_result(f"每次抽奖消耗已设置为 {cost_points} 积分。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置兑换积分")
    async def admin_set_redeem_cost(self, event: AstrMessageEvent, cost_points: int):
        """管理员设置固定兑换所需积分。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if cost_points <= 0:
            yield event.plain_result("兑换积分必须大于 0。")
            return

        data = await self._load_data()
        data["settings"]["redeem_cost"] = cost_points
        await self._save_data(data)
        yield event.plain_result(f"兑换所需积分已设置为 {cost_points}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置兑换通知")
    async def admin_set_redeem_notify(self, event: AstrMessageEvent, notify_qq: str):
        """管理员设置兑换提醒 QQ。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        notify_qq = self._normalize_qq(notify_qq)
        if not notify_qq:
            yield event.plain_result("通知 QQ 无效，请输入纯QQ号或包含QQ号的文本。")
            return

        data = await self._load_data()
        data["settings"]["redeem_notify_qq"] = notify_qq
        await self._save_data(data)
        yield event.plain_result(f"兑换通知 QQ 已设置为 {notify_qq}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置保底")
    async def admin_set_lottery_pity(self, event: AstrMessageEvent, pity_threshold: int):
        """管理员设置抽奖保底阈值（连续未中奖次数）。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if pity_threshold < 0:
            yield event.plain_result("保底阈值不能小于 0。")
            return

        data = await self._load_data()
        data["settings"]["lottery_pity_threshold"] = pity_threshold
        await self._save_data(data)

        if pity_threshold == 0:
            yield event.plain_result("抽奖保底已关闭。")
            return
        yield event.plain_result(f"抽奖保底已设置：连续 {pity_threshold} 次未中奖，下次必出三等奖。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("兑换处理")
    async def admin_redeem_processing(self, event: AstrMessageEvent, order_id: int, note: str = ""):
        """管理员将兑换单标记为已处理。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        data = await self._load_data()
        order = self._find_redeem_order(data, order_id)
        if not order:
            yield event.plain_result(f"未找到兑换单 #{order_id}。")
            return

        if order.get("status") == "已完成":
            yield event.plain_result(f"兑换单 #{order_id} 已完成，无需重复处理。")
            return

        handler_id = str(event.get_sender_id())
        handler_name = event.get_sender_name() or handler_id
        order["status"] = "已处理"
        order["updated_at"] = self._now_str()
        order["handler_id"] = handler_id
        order["handler_name"] = handler_name
        if note:
            order["note"] = note

        await self._save_data(data)
        yield event.plain_result(f"兑换单 #{order_id} 状态已更新为 已处理。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("同意兑换", alias={"兑换同意"})
    async def admin_redeem_approve(self, event: AstrMessageEvent, order_id: int, note: str = ""):
        """管理员同意兑换（等价于兑换处理）。"""
        async for result in self.admin_redeem_processing(event, order_id, note):
            yield result

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("兑换完成")
    async def admin_redeem_completed(self, event: AstrMessageEvent, order_id: int, note: str = ""):
        """管理员将兑换单标记为已完成。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        data = await self._load_data()
        order = self._find_redeem_order(data, order_id)
        if not order:
            yield event.plain_result(f"未找到兑换单 #{order_id}。")
            return

        handler_id = str(event.get_sender_id())
        handler_name = event.get_sender_name() or handler_id
        order["status"] = "已完成"
        order["updated_at"] = self._now_str()
        order["handler_id"] = handler_id
        order["handler_name"] = handler_name
        if note:
            order["note"] = note

        await self._save_data(data)
        yield event.plain_result(f"兑换单 #{order_id} 状态已更新为 已完成。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("兑换待处理")
    async def admin_redeem_pending(self, event: AstrMessageEvent, limit: int = 10):
        """管理员查看待处理兑换单（状态：已申请）。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        if limit <= 0:
            limit = 10
        limit = min(limit, 50)

        data = await self._load_data()
        pending_orders: list[dict[str, Any]] = []
        for item in data["redeems"]:
            if not isinstance(item, dict):
                continue
            if str(item.get("status", "")) == "已申请":
                pending_orders.append(item)

        if not pending_orders:
            yield event.plain_result("当前没有待处理兑换单。")
            return

        pending_orders = pending_orders[-limit:]
        lines = [f"待处理兑换单（最近 {len(pending_orders)} 条）："]
        for item in pending_orders:
            lines.append(
                f"#{item.get('id', 0)} | {item.get('user_name', '')}({item.get('user_id', '')})"
                f" | {item.get('cost', 0)}积分 | {item.get('created_at', '')}"
            )
        yield event.plain_result("\n".join(lines))

    @filter.command("设置")
    async def show_settings(self, event: AstrMessageEvent):
        """查看当前随机奖励配置。"""
        blocked = self._blocked_result(event)
        if blocked is not None:
            yield blocked
            return

        data = await self._load_data()
        chance, reward_min, reward_max = self._get_reward_settings(data)
        cooldown = self._cfg_int("chat_reward_cooldown_seconds", 30, 0, 86400)
        sign_points = self._cfg_int("sign_in_points", 8, 1, 10000)
        streak_bonus_per_day = self._cfg_int("sign_in_streak_bonus_per_day", 2, 0, 10000)
        streak_bonus_max = self._cfg_int("sign_in_streak_bonus_max", 30, 0, 100000)
        lucky_chance, lucky_min, lucky_max = self._get_sign_in_lucky_settings(data)
        lottery = self._get_lottery_settings(data)
        pity_threshold = self._get_lottery_pity_threshold(data)
        redeem_cost, redeem_notify_qq = self._get_redeem_settings(data)
        p1, p2, p3 = lottery["prizes"]
        yield event.plain_result(
            "当前积分配置：\n"
            f"- 聊天触发概率：{chance}%\n"
            f"- 随机奖励范围：{reward_min}-{reward_max}\n"
            f"- 聊天奖励冷却：{cooldown} 秒\n"
            f"- 群成员互聊可触发奖励：{'是' if self._cfg_bool('group_chat_reward_for_members', True) else '否'}\n"
            f"- 每日签到基础奖励：{sign_points}\n"
            f"- 连签每日加成：{streak_bonus_per_day}（上限 {streak_bonus_max}）\n"
            f"- 签到幸运加成概率：{lucky_chance}%（触发时 +{lucky_min}-{lucky_max}）\n"
            f"- 每次抽奖消耗：{lottery['cost']}\n"
            f"- 一等奖：{p1[1]} 积分，概率 {p1[2]}%\n"
            f"- 二等奖：{p2[1]} 积分，概率 {p2[2]}%\n"
            f"- 三等奖：{p3[1]} 积分，概率 {p3[2]}%\n"
            f"- 抽奖保底阈值：{pity_threshold}（0 为关闭）\n"
            f"- 奖项总概率：{lottery['total_chance']}%\n"
            f"- 固定兑换积分：{redeem_cost}\n"
            f"- 兑换通知 QQ：{redeem_notify_qq or '未设置'}"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("看板地址")
    async def show_dashboard_url(self, event: AstrMessageEvent):
        """管理员查看本地积分看板地址。"""
        if self._dashboard_runner is None:
            await self._start_dashboard_server()

        token = self._cfg_str("dashboard_token", "").strip()
        if self._dashboard_url:
            if token:
                yield event.plain_result(
                    f"积分看板地址：{self._dashboard_url}/?token={token}"
                )
                return
            yield event.plain_result(f"积分看板地址：{self._dashboard_url}")
            return

        if self._dashboard_error:
            yield event.plain_result(f"积分看板未启动：{self._dashboard_error}")
            return

        yield event.plain_result("积分看板未启用，请检查 dashboard_enabled 配置。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("重启看板")
    async def restart_dashboard(self, event: AstrMessageEvent):
        """管理员重启本地积分看板服务。"""
        await self._stop_dashboard_server()
        await self._start_dashboard_server()

        token = self._cfg_str("dashboard_token", "").strip()
        if self._dashboard_url:
            if token:
                yield event.plain_result(f"积分看板已重启：{self._dashboard_url}/?token={token}")
                return
            yield event.plain_result(f"积分看板已重启：{self._dashboard_url}")
            return

        if self._dashboard_error:
            yield event.plain_result(f"积分看板重启失败：{self._dashboard_error}")
            return

        yield event.plain_result("积分看板未启用，请检查 dashboard_enabled 配置。")

    async def terminate(self):
        """插件被卸载/停用时调用。"""
        await self._stop_dashboard_server()
