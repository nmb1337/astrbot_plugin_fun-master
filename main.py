from datetime import date, timedelta
from html import escape
import random
import re
import time
from typing import Any

import astrbot.api.message_components as Comp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

try:
    from aiohttp import web
except ImportError:
    web = None


@register("astrbot_plugin_fun", "Copilot", "聊天积分玩法插件", "1.0.0")
class PointsPlugin(Star):
    DATA_KEY = "points_data_v1"

    def __init__(self, context: Context, config: Any = None):
        super().__init__(context)
        self.config = config or {}
        self._dashboard_runner = None
        self._dashboard_url = ""
        self._dashboard_error = ""

    async def initialize(self):
        """插件初始化时自动调用。"""
        data = await self._load_data()
        self._refresh_webui_snapshot(data)
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

    async def _load_data(self) -> dict[str, Any]:
        data = await self.get_kv_data(self.DATA_KEY, {})
        if not isinstance(data, dict):
            data = {}

        users = data.get("users", {})
        settings = data.get("settings", {})
        chat_ts = data.get("chat_ts", {})
        redeems = data.get("redeems", [])
        redeem_seq = self._to_int(data.get("redeem_seq", 0), 0)
        if not isinstance(users, dict):
            users = {}
        if not isinstance(settings, dict):
            settings = {}
        if not isinstance(chat_ts, dict):
            chat_ts = {}
        if not isinstance(redeems, list):
            redeems = []

        return {
            "users": users,
            "settings": settings,
            "chat_ts": chat_ts,
            "redeems": redeems,
            "redeem_seq": max(0, redeem_seq),
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

    def _refresh_webui_snapshot(self, data: dict[str, Any]) -> None:
        max_lines = self._cfg_int("webui_snapshot_max_lines", 300, 50, 5000)

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

        points_lines = [
            "# 积分看板",
            f"更新时间: {self._now_str()}",
            f"总用户数: {len(users)}",
            "",
            "排名 | 用户 | QQ | 积分 | 连签",
            "---|---|---|---|---",
        ]
        for idx, (uid, name, points, streak) in enumerate(users[:max_lines], start=1):
            points_lines.append(f"{idx} | {name} | {uid} | {points} | {streak}")
        if len(users) > max_lines:
            points_lines.append("")
            points_lines.append(f"仅显示前 {max_lines} 条，其余请调整 webui_snapshot_max_lines。")

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
        redeem_lines = [
            "# 兑换记录看板",
            f"更新时间: {self._now_str()}",
            f"总记录数: {len(records)}",
            f"已申请: {status_counts['已申请']} | 已处理: {status_counts['已处理']} | 已完成: {status_counts['已完成']}",
            "",
            "单号 | 状态 | 用户 | QQ | 积分 | 说明 | 更新时间 | 处理人",
            "---|---|---|---|---|---|---|---",
        ]
        for item in records[:max_lines]:
            order_id = self._to_int(item.get("id", 0), 0)
            status = str(item.get("status", "未知"))
            user_name = str(item.get("user_name", ""))
            user_id = str(item.get("user_id", ""))
            cost = self._to_int(item.get("cost", 0), 0)
            reason = str(item.get("reason", "")).replace("\n", " ")[:50]
            updated_at = str(item.get("updated_at", ""))
            handler_name = str(item.get("handler_name", ""))
            redeem_lines.append(
                f"{order_id} | {status} | {user_name} | {user_id} | {cost} | {reason} | {updated_at} | {handler_name}"
            )
        if len(records) > max_lines:
            redeem_lines.append("")
            redeem_lines.append(f"仅显示最近 {max_lines} 条，其余请调整 webui_snapshot_max_lines。")

        if isinstance(self.config, dict):
            self.config["webui_points_snapshot"] = "\n".join(points_lines)
            self.config["webui_redeem_snapshot"] = "\n".join(redeem_lines)

        save_fn = getattr(self.config, "save_config", None)
        if callable(save_fn):
            try:
                save_fn()
            except Exception as exc:
                logger.warning(f"刷新 WebUI 看板失败: {exc}")

    async def _start_dashboard_server(self) -> None:
        if not self._cfg_bool("dashboard_enabled", True):
            self._dashboard_url = ""
            self._dashboard_error = ""
            return

        if web is None:
            self._dashboard_error = "未安装 aiohttp，无法启动积分看板。"
            logger.warning(self._dashboard_error)
            return

        host = self._cfg_str("dashboard_host", "127.0.0.1").strip() or "127.0.0.1"
        port = self._cfg_int("dashboard_port", 6666, 1, 65535)

        app = web.Application()
        app.router.add_get("/", self._dashboard_index)
        app.router.add_get("/healthz", self._dashboard_health)

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

    async def _dashboard_index(self, request):
        token = self._cfg_str("dashboard_token", "").strip()
        if token and request.query.get("token", "") != token:
            return web.Response(status=401, text="Unauthorized")

        data = await self._load_data()
        html_text = self._build_dashboard_html(data)
        return web.Response(text=html_text, content_type="text/html")

    def _build_dashboard_html(self, data: dict[str, Any]) -> str:
        title = escape(self._cfg_str("dashboard_title", "积分看板"))
        refresh_seconds = self._cfg_int("dashboard_auto_refresh_seconds", 15, 0, 3600)
        refresh_meta = ""
        if refresh_seconds > 0:
            refresh_meta = f'<meta http-equiv="refresh" content="{refresh_seconds}">'

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
            status = escape(str(item.get("status", "未知")))
            user_name = escape(str(item.get("user_name", "")))
            user_id = escape(str(item.get("user_id", "")))
            cost = self._to_int(item.get("cost", 0), 0)
            reason = escape(str(item.get("reason", "")).replace("\n", " "))
            updated_at = escape(str(item.get("updated_at", "")))
            handler_name = escape(str(item.get("handler_name", "")))
            redeem_rows.append(
                "<tr>"
                f"<td>{order_id}</td><td>{status}</td><td>{user_name}</td><td>{user_id}</td>"
                f"<td>{cost}</td><td>{reason}</td><td>{updated_at}</td><td>{handler_name}</td>"
                "</tr>"
            )
        if not redeem_rows:
            redeem_rows.append('<tr><td colspan="8">暂无兑换记录</td></tr>')

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
            "</style></head><body>"
            f"<div class=\"card\"><h1>{title}</h1>"
            f"<div class=\"meta\">更新时间：{escape(self._now_str())} | 用户数：{len(users)} | 兑换单：{len(records)}</div>"
            f"<div class=\"meta\">状态统计：已申请 {status_counts['已申请']}，已处理 {status_counts['已处理']}，已完成 {status_counts['已完成']}</div>"
            "</div>"
            "<div class=\"card\"><h2>每个人的积分</h2>"
            "<table><thead><tr><th>排名</th><th>昵称</th><th>QQ</th><th>积分</th><th>连签</th></tr></thead>"
            f"<tbody>{''.join(points_rows)}</tbody></table></div>"
            "<div class=\"card\"><h2>兑换申请与记录</h2>"
            "<table><thead><tr><th>单号</th><th>状态</th><th>申请人</th><th>QQ</th><th>积分</th><th>说明</th><th>更新时间</th><th>处理人</th></tr></thead>"
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

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def random_chat_reward(self, event: AstrMessageEvent):
        """聊天概率触发随机积分奖励。"""
        message_str = (event.message_str or "").strip()
        if not message_str:
            return
        if message_str.startswith("/"):
            return

        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name() or sender_id

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
        self._refresh_webui_snapshot(data)
        yield event.plain_result(
            f"随机奖励触发，{sender_name} 获得 {gained} 积分，当前积分 {user['points']}。"
        )

    @filter.command("签到")
    async def sign_in(self, event: AstrMessageEvent):
        """每日签到领取基础积分。"""
        sender_id = str(event.get_sender_id())
        sender_name = event.get_sender_name() or sender_id
        today = date.today().isoformat()

        data = await self._load_data()
        user = self._ensure_user(data, sender_id, sender_name)
        if user.get("last_sign_date") == today:
            yield event.plain_result(f"{sender_name} 今天已经签过到了，当前积分 {user['points']}。")
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

        extra_bonus = max(0, streak - 1) * streak_bonus_per_day
        extra_bonus = min(extra_bonus, streak_bonus_max)
        sign_points = base_points + extra_bonus

        self._change_points(user, sign_points)
        user["last_sign_date"] = today
        user["sign_streak"] = streak
        await self._save_data(data)
        self._refresh_webui_snapshot(data)
        yield event.plain_result(
            f"签到成功，{sender_name} 连续签到 {streak} 天，"
            f"基础 {base_points} + 连签加成 {extra_bonus} = {sign_points} 积分，当前积分 {user['points']}。"
        )

    @filter.command("查询", alias={"余额", "我的积分"})
    async def query_points(self, event: AstrMessageEvent, qq: str = ""):
        """查询自己或指定 QQ 的积分。"""
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
        self._refresh_webui_snapshot(data)

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
        self._refresh_webui_snapshot(data)

        notify_text = (
            f"兑换申请 #{order_id}（状态：已申请）：{sender_name}({sender_id})"
            f" 使用 {redeem_cost} 积分，说明：{reason}。剩余积分 {sender_user['points']}。"
        )

        if notify_qq:
            yield event.chain_result(self._build_notify_chain(notify_qq, notify_text))
            return

        yield event.plain_result(f"兑换申请已创建，但未识别到有效通知QQ。{notify_text}")

    @filter.command("兑换状态")
    async def redeem_status(self, event: AstrMessageEvent, order_id: int = 0):
        """查询兑换审核状态。"""
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
    @filter.command("加分")
    async def admin_add_points(self, event: AstrMessageEvent, qq: str, points: int):
        """管理员给指定 QQ 增加积分。"""
        if points <= 0:
            yield event.plain_result("加分数值必须大于 0。")
            return

        data = await self._load_data()
        user = self._ensure_user(data, qq)
        self._change_points(user, points)
        await self._save_data(data)
        self._refresh_webui_snapshot(data)
        yield event.plain_result(f"已为 {qq} 增加 {points} 积分，当前 {user['points']}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("扣分")
    async def admin_sub_points(self, event: AstrMessageEvent, qq: str, points: int):
        """管理员扣除指定 QQ 的积分。"""
        if points <= 0:
            yield event.plain_result("扣分数值必须大于 0。")
            return

        data = await self._load_data()
        user = self._ensure_user(data, qq)
        self._change_points(user, -points)
        await self._save_data(data)
        self._refresh_webui_snapshot(data)
        yield event.plain_result(f"已扣除 {qq} {points} 积分，当前 {user['points']}。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("设置概率")
    async def admin_set_chance(self, event: AstrMessageEvent, chance_percent: int):
        """管理员设置聊天随机奖励概率。"""
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
        self._refresh_webui_snapshot(data)
        yield event.plain_result(f"兑换单 #{order_id} 状态已更新为 已处理。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("兑换完成")
    async def admin_redeem_completed(self, event: AstrMessageEvent, order_id: int, note: str = ""):
        """管理员将兑换单标记为已完成。"""
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
        self._refresh_webui_snapshot(data)
        yield event.plain_result(f"兑换单 #{order_id} 状态已更新为 已完成。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("兑换待处理")
    async def admin_redeem_pending(self, event: AstrMessageEvent, limit: int = 10):
        """管理员查看待处理兑换单（状态：已申请）。"""
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
        data = await self._load_data()
        chance, reward_min, reward_max = self._get_reward_settings(data)
        cooldown = self._cfg_int("chat_reward_cooldown_seconds", 30, 0, 86400)
        sign_points = self._cfg_int("sign_in_points", 8, 1, 10000)
        streak_bonus_per_day = self._cfg_int("sign_in_streak_bonus_per_day", 2, 0, 10000)
        streak_bonus_max = self._cfg_int("sign_in_streak_bonus_max", 30, 0, 100000)
        lottery = self._get_lottery_settings(data)
        pity_threshold = self._get_lottery_pity_threshold(data)
        redeem_cost, redeem_notify_qq = self._get_redeem_settings(data)
        p1, p2, p3 = lottery["prizes"]
        yield event.plain_result(
            "当前积分配置：\n"
            f"- 聊天触发概率：{chance}%\n"
            f"- 随机奖励范围：{reward_min}-{reward_max}\n"
            f"- 聊天奖励冷却：{cooldown} 秒\n"
            f"- 每日签到基础奖励：{sign_points}\n"
            f"- 连签每日加成：{streak_bonus_per_day}（上限 {streak_bonus_max}）\n"
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
    @filter.command("刷新看板")
    async def refresh_webui_board(self, event: AstrMessageEvent):
        """管理员手动刷新 WebUI 看板数据。"""
        data = await self._load_data()
        self._refresh_webui_snapshot(data)
        yield event.plain_result("WebUI 看板已刷新，请在插件配置页查看积分与兑换记录。")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("看板地址")
    async def show_dashboard_url(self, event: AstrMessageEvent):
        """管理员查看本地积分看板地址。"""
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

    async def terminate(self):
        """插件被卸载/停用时调用。"""
        await self._stop_dashboard_server()
