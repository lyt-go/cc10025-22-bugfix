"""cost_tracker 领域服务：成本追踪管理，接收 asset_catalog/cold_storage/hot_prefetch/backfill_job 上游事件。

跨模块数据流：
    asset_catalog → cost_tracker: 资产发布创建成本种子
    cold_storage → cost_tracker: 冷存迁移更新存储成本
    hot_prefetch → cost_tracker: 回迁启动提交传输证据，核对任务来源后入账或生成更正分录
    partition_plan → backfill_job → cost_tracker: 补数更新计算成本

传输费用账本约定：
    - 月报（settle_monthly_report）一经结算即「已发送」，作为原账按当时口径永久保留、可回查；
    - 回迁入口提交的传输证据若证明费用实际属于其他资产，生成「回迁费用更正分录」：
      原月报不改写，更正金额计入下一次付款汇总，两个资产的传输指标与有效余额随之重算；
    - 同一物理传输（按任务编号/冷存位置识别）只入账一次，并发启动与重复核算均幂等；
    - 月报与更正分录持久化到 app/data/assets/cost_monthly_*.json / cost_correction_*.json。
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any

from app.services.base import (
    BaseModuleService,
    asset_id_from_location,
    location_matches_asset,
)
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")

TRANSFER_RATE_PER_GB = 0.09  # 传输单价：元/GB，100GB → 9.00


def fee_for_volume(volume: str) -> float:
    """按回迁量计算传输费。"""
    text = str(volume or "").strip()
    try:
        if "GB" in text:
            return round(float(text.replace("GB", "")) * TRANSFER_RATE_PER_GB, 2)
        return round(float(text) * TRANSFER_RATE_PER_GB, 2) if text else 0.0
    except ValueError:
        return 0.0


def _to_float(value: Any, default: float | None) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _current_period() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _next_period(period: str) -> str:
    year, month = int(period[:4]), int(period[5:7]) + 1
    if month > 12:
        year, month = year + 1, 1
    return f"{year:04d}-{month:02d}"


class CostTrackerService(BaseModuleService):
    MODULE = "cost_tracker"
    ENTITY = "成本记录"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["记录编号", "关联资产", "存储成本", "计算成本", "传输成本", "统计周期", "趋势方向", "异常标记"]
    REQUIRED_FIELDS = ["记录编号", "关联资产", "存储成本"]
    STATUS_ORDER = ["草稿", "已核算", "已确认", "已归档"]
    ACTION_RULES = {"提交核算": "已核算", "确认成本": "已确认", "标记归档": "已归档"}
    NEGATIVE_ACTIONS = []

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)
        self._lock = threading.RLock()
        self._monthly_reports: list[dict[str, Any]] = []   # 已结算月报（原账，不可改写）
        self._corrections: list[dict[str, Any]] = []       # 回迁费用更正分录
        self._transfer_charges: list[dict[str, Any]] = []  # 未结传输费用（月结前在账）
        self._load_ledger()

    # ---------- 原有核算动作 ----------
    def calculate_cost(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """核算成本：汇总存储、计算、传输三项成本。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("关联资产", ""))

        # 从其他模块收集成本信息
        storage_cost = self._estimate_storage_cost(asset_id)
        compute_cost = self._estimate_compute_cost(asset_id)
        transfer_cost = self._estimate_transfer_cost(asset_id)

        entry["存储成本"] = f"{storage_cost:.2f}"
        entry["计算成本"] = f"{compute_cost:.2f}"
        entry["传输成本"] = f"{transfer_cost:.2f}"

        total = storage_cost + compute_cost + transfer_cost
        entry["趋势方向"] = "上升" if total > 100 else "持平"
        entry["异常标记"] = "异常" if total > 500 else "无"
        entry["status"] = "已核算"
        entry["pending"] = False

        # 写入成本报告
        self._write_cost_report(asset_id, entry)

        return entry, f"{self.ENTITY} {entry_id} 已核算，总成本 {total:.2f}"

    def get_cost_report(self, asset_id: str) -> dict[str, Any] | None:
        """读取某资产的成本报告。"""
        filename = f"cost_report_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "提交核算":
            return self.calculate_cost(entry_id)
        return super().run_action(entry_id, action)

    # ---------- 传输费用入账 ----------
    def book_transfer_charge(self, asset_id: str, *, task_id: str,
                             cold_location: str, volume: str) -> dict[str, Any]:
        """把一笔回迁传输费登记到指定资产的未结费用（月结前的在账费用）。

        同一任务编号只登记一次；若该来源已结入月报，则不再重复入账。
        """
        asset_id = str(asset_id or "").strip()
        task_id = str(task_id or "").strip()
        cold_location = str(cold_location or "").strip()
        fee = fee_for_volume(volume)
        with self._lock:
            if self._source_in_reports(task_id, cold_location) is not None:
                # 该物理传输已结入月报，不得重复入账
                self._refresh_transfer_metric(asset_id)
                return {"资产": asset_id, "任务编号": task_id, "费用": fee, "已结": True}
            charge = self._find_open_charge(task_id)
            if charge is None:
                charge = {
                    "资产": asset_id,
                    "任务编号": task_id,
                    "冷存位置": cold_location,
                    "回迁量": str(volume or ""),
                    "费用": fee,
                    "期间": _current_period(),
                    "已结": False,
                }
                self._transfer_charges.append(charge)
            row = self._row_for_asset(asset_id)
            if row is not None:
                sources = row.setdefault("传输来源", [])
                if not any(s.get("任务编号") == task_id for s in sources):
                    sources.append({
                        "任务编号": task_id,
                        "冷存位置": cold_location,
                        "回迁量": str(volume or ""),
                        "费用": fee,
                    })
            self._refresh_transfer_metric(asset_id)
            return charge

    def settle_monthly_report(self, asset_id: str,
                              period: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """月结：把资产当前传输费结入月报。月报一经结算即「已发送」，不可改写、可回查。"""
        asset_id = str(asset_id or "").strip()
        period = period or _current_period()
        with self._lock:
            for report in self._monthly_reports:
                if report["资产"] == asset_id and report["期间"] == period:
                    return report, f"{asset_id} {period} 月报已结算（幂等返回原账）"
            row = self._row_for_asset(asset_id)
            if row is None:
                return None, f"资产 {asset_id} 没有成本记录，无法月结"
            transfer = _to_float(row.get("传输成本"), None)
            if transfer is None:
                transfer = sum(ch["费用"] for ch in self._transfer_charges
                               if ch["资产"] == asset_id and not ch["已结"])
            report = {
                "月报编号": f"MR-{asset_id}-{period}",
                "资产": asset_id,
                "期间": period,
                "存储成本": _to_float(row.get("存储成本"), 0.0) or 0.0,
                "计算成本": _to_float(row.get("计算成本"), 0.0) or 0.0,
                "传输成本": round(transfer, 2),
                "传输来源": [dict(s) for s in row.get("传输来源", [])],
                "状态": "已发送",
                "settled_at": datetime.now(timezone.utc).isoformat(),
            }
            self._monthly_reports.append(report)
            for charge in self._transfer_charges:
                if charge["资产"] == asset_id and not charge["已结"]:
                    charge["已结"] = True
            self._persist_monthly_report(report)
            return report, f"{asset_id} {period} 月报已结算并发送，传输费 {report['传输成本']:.2f}"

    def record_prefetch_transfer(self, *, task_id: str, cold_location: str,
                                 volume: str, target_partition: str = "") -> dict[str, Any]:
        """回迁入口提交传输证据：核对任务来源后入账或生成回迁费用更正分录。

        幂等：同一物理传输（任务编号/冷存位置）只入账一次，
        并发启动与重复核算都不会各自再扣一笔。
        """
        task_id = str(task_id or "").strip()
        cold_location = str(cold_location or "").strip()
        asset_id = asset_id_from_location(cold_location)
        fee = fee_for_volume(volume)
        evidence = {
            "任务编号": task_id,
            "冷存位置": cold_location,
            "回迁量": str(volume or ""),
            "目标分区": str(target_partition or ""),
            "证据费用": fee,
        }
        with self._lock:
            # 1) 该任务已生成过更正分录 → 幂等，仅按账本重算两资产传输指标
            for correction in self._corrections:
                if correction["依据"].get("任务编号") == task_id:
                    self._refresh_transfer_metric(correction["原资产"])
                    self._refresh_transfer_metric(correction["实际资产"])
                    return {
                        "ok": True, "deduped": True, "correction": correction,
                        "message": f"任务 {task_id} 已生成更正分录 {correction['更正编号']}，物理传输只算一次，幂等跳过",
                    }
            # 2) 未结费用中已有该任务 → 不重复扣费；归属错了就在月结前直接改归属
            charge = self._find_open_charge(task_id)
            if charge is not None:
                if asset_id and charge["资产"] != asset_id:
                    previous = charge["资产"]
                    charge["资产"] = asset_id
                    self._refresh_transfer_metric(previous)
                    self._refresh_transfer_metric(asset_id)
                    return {
                        "ok": True, "reassigned": True, "charge": charge,
                        "message": f"任务 {task_id} 未结费用归属由 {previous} 改记 {asset_id}，不重复扣费",
                    }
                return {
                    "ok": True, "deduped": True, "charge": charge,
                    "message": f"任务 {task_id} 传输费已在账，幂等跳过",
                }
            # 3) 已结月报中含该来源 → 核对归属，错账生成回迁费用更正分录
            found = self._source_in_reports(task_id, cold_location)
            if found is not None:
                report, source = found
                booked_asset = report["资产"]
                if booked_asset == asset_id:
                    return {
                        "ok": True, "deduped": True, "settled": report["月报编号"],
                        "message": f"任务 {task_id} 传输费已正确结入 {report['月报编号']}，幂等跳过",
                    }
                settled_fee = _to_float(source.get("费用"), fee) or 0.0
                correction = self._create_correction(
                    from_asset=booked_asset,
                    to_asset=asset_id,
                    amount=settled_fee,
                    source_period=report["期间"],
                    evidence=evidence,
                    settled_fee=settled_fee,
                )
                # 用更正分录重算两个资产的传输指标
                self._refresh_transfer_metric(booked_asset)
                self._refresh_transfer_metric(asset_id)
                return {
                    "ok": True, "correction": correction,
                    "message": (f"任务来源核对完成：{cold_location} 实际属于 {asset_id}，"
                                f"已生成回迁费用更正分录 {correction['更正编号']}，"
                                f"{correction['金额']:.2f} 由 {booked_asset} 更正到 {asset_id}，"
                                f"计入 {correction['生效期间']} 付款汇总"),
                }
            # 4) 全新传输：登记未结费用（无成本记录则跳过，避免垃圾入账）
            if not asset_id or self._row_for_asset(asset_id) is None:
                return {
                    "ok": False, "skipped": True,
                    "message": f"冷存位置 {cold_location} 未匹配到资产成本记录，传输费未入账",
                }
            charge = self.book_transfer_charge(
                asset_id, task_id=task_id, cold_location=cold_location, volume=volume)
            return {
                "ok": True, "charge": charge,
                "message": f"任务 {task_id} 传输费 {fee:.2f} 已计入 {asset_id}",
            }

    # ---------- 账本查询（回查） ----------
    def list_corrections(self, asset_id: str | None = None) -> list[dict[str, Any]]:
        """回迁费用更正分录（修订依据）回查。"""
        if asset_id is None:
            return [dict(c) for c in self._corrections]
        return [dict(c) for c in self._corrections
                if c["原资产"] == asset_id or c["实际资产"] == asset_id]

    def get_monthly_report(self, asset_id: str, period: str) -> dict[str, Any] | None:
        """按当时口径回查已发送的原月报。"""
        for report in self._monthly_reports:
            if report["资产"] == asset_id and report["期间"] == period:
                return dict(report)
        return None

    def list_monthly_reports(self, asset_id: str | None = None) -> list[dict[str, Any]]:
        if asset_id is None:
            return [dict(r) for r in self._monthly_reports]
        return [dict(r) for r in self._monthly_reports if r["资产"] == asset_id]

    def get_payment_summary(self, period: str | None = None) -> dict[str, Any]:
        """付款汇总：指定期间的已结传输费 + 生效于该期间的更正调整 + 未结费用。"""
        period = period or _current_period()
        items: dict[str, dict[str, Any]] = {}

        def item(asset: str) -> dict[str, Any]:
            return items.setdefault(asset, {
                "资产": asset, "已结传输费": 0.0, "更正调整": 0.0,
                "未结传输费": 0.0, "应付": 0.0,
            })

        for report in self._monthly_reports:
            if report["期间"] == period:
                item(report["资产"])["已结传输费"] += report["传输成本"]
        for correction in self._corrections:
            if correction["生效期间"] == period:
                item(correction["原资产"])["更正调整"] -= correction["金额"]
                item(correction["实际资产"])["更正调整"] += correction["金额"]
        for charge in self._transfer_charges:
            if not charge["已结"] and charge["期间"] == period:
                item(charge["资产"])["未结传输费"] += charge["费用"]
        for it in items.values():
            it["应付"] = round(it["已结传输费"] + it["更正调整"] + it["未结传输费"], 2)
        return {
            "期间": period,
            "items": items,
            "合计应付": round(sum(it["应付"] for it in items.values()), 2),
        }

    def get_asset_balance(self, asset_id: str) -> dict[str, Any]:
        """资产有效余额：全部已结月报 + 更正调整 + 未结费用。错账不得残留。"""
        settled = sum(r["传输成本"] for r in self._monthly_reports
                      if r["资产"] == asset_id)
        adjust = 0.0
        for correction in self._corrections:
            if correction["原资产"] == asset_id:
                adjust -= correction["金额"]
            if correction["实际资产"] == asset_id:
                adjust += correction["金额"]
        open_amount = sum(ch["费用"] for ch in self._transfer_charges
                          if ch["资产"] == asset_id and not ch["已结"])
        return {
            "资产": asset_id,
            "已结传输费": round(settled, 2),
            "更正调整": round(adjust, 2),
            "未结传输费": round(open_amount, 2),
            "有效余额": round(settled + adjust + open_amount, 2),
        }

    # ---------- 账本内部 ----------
    @staticmethod
    def _row_for_asset(asset_id: str) -> dict[str, Any] | None:
        for row in store.rows("cost_tracker"):
            if str(row.get("关联资产", "")) == asset_id:
                return row
        return None

    def _find_open_charge(self, task_id: str) -> dict[str, Any] | None:
        for charge in self._transfer_charges:
            if charge["任务编号"] == task_id and not charge["已结"]:
                return charge
        return None

    def _source_in_reports(self, task_id: str, cold_location: str):
        """在已结月报的传输来源里查找同一物理传输。"""
        for report in self._monthly_reports:
            for source in report.get("传输来源", []):
                if source.get("任务编号") == task_id:
                    return report, source
                if cold_location and source.get("冷存位置") == cold_location:
                    return report, source
        return None

    def _create_correction(self, *, from_asset: str, to_asset: str, amount: float,
                           source_period: str, evidence: dict[str, Any],
                           settled_fee: float) -> dict[str, Any]:
        number = len(self._corrections) + 1
        correction = {
            "更正编号": f"CORR-{number:04d}",
            "类型": "回迁费用更正",
            "原资产": from_asset,
            "实际资产": to_asset,
            "金额": round(amount, 2),
            "原账期间": source_period,
            "生效期间": _next_period(source_period),
            "依据": {**evidence, "原账费用": round(settled_fee, 2)},
            "状态": "已生效",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._corrections.append(correction)
        self._persist_correction(correction)
        return correction

    def _effective_transfer(self, asset_id: str) -> float | None:
        """账本口径的有效传输费；该资产没有任何账本事件时返回 None。"""
        has_event = False
        total = 0.0
        for report in self._monthly_reports:
            if report["资产"] == asset_id:
                total += report["传输成本"]
                has_event = True
        for correction in self._corrections:
            if correction["原资产"] == asset_id:
                total -= correction["金额"]
                has_event = True
            if correction["实际资产"] == asset_id:
                total += correction["金额"]
                has_event = True
        for charge in self._transfer_charges:
            if charge["资产"] == asset_id and not charge["已结"]:
                total += charge["费用"]
                has_event = True
        return round(total, 2) if has_event else None

    def _refresh_transfer_metric(self, asset_id: str) -> None:
        """用账本有效额重算成本记录上的传输指标。"""
        effective = self._effective_transfer(asset_id)
        if effective is None:
            return
        row = self._row_for_asset(asset_id)
        if row is not None:
            row["传输成本"] = f"{effective:.2f}"

    # ---------- 账本持久化 ----------
    def _persist_monthly_report(self, report: dict[str, Any]) -> None:
        path = os.path.join(
            DATA_DIR, f"cost_monthly_{report['资产']}_{report['期间']}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    def _persist_correction(self, correction: dict[str, Any]) -> None:
        path = os.path.join(DATA_DIR, f"cost_correction_{correction['更正编号']}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(correction, f, ensure_ascii=False, indent=2, default=str)

    def _load_ledger(self) -> None:
        if not os.path.isdir(DATA_DIR):
            return
        for name in sorted(os.listdir(DATA_DIR)):
            path = os.path.join(DATA_DIR, name)
            try:
                if name.startswith("cost_monthly_") and name.endswith(".json"):
                    with open(path, "r", encoding="utf-8") as f:
                        self._monthly_reports.append(json.load(f))
                elif name.startswith("cost_correction_") and name.endswith(".json"):
                    with open(path, "r", encoding="utf-8") as f:
                        self._corrections.append(json.load(f))
            except (OSError, ValueError):
                continue

    # ---------- 原有估算逻辑 ----------
    def _estimate_storage_cost(self, asset_id: str) -> float:
        """从冷存储任务估算存储成本。"""
        cold_rows = store.rows("cold_storage")
        for row in cold_rows:
            if str(row.get("源资产")) == asset_id:
                vol = str(row.get("迁移量", "0"))
                try:
                    return float(vol.replace("GB", "")) * 0.02 if "GB" in vol else 10.0
                except ValueError:
                    pass
        return 5.0

    def _estimate_compute_cost(self, asset_id: str) -> float:
        """从补数任务估算计算成本。"""
        backfill_rows = store.rows("backfill_job")
        for row in backfill_rows:
            if str(row.get("目标资产")) == asset_id:
                vol = str(row.get("数据量", "0"))
                try:
                    return float(vol.replace("GB", "")) * 0.05 if "GB" in vol else 20.0
                except ValueError:
                    pass
        return 10.0

    def _estimate_transfer_cost(self, asset_id: str) -> float:
        """传输成本：优先取账本有效额；否则按冷存位置精确匹配回迁任务估算。"""
        effective = self._effective_transfer(asset_id)
        if effective is not None:
            return effective
        hot_rows = store.rows("hot_prefetch")
        for row in hot_rows:
            loc = str(row.get("冷存位置", ""))
            if location_matches_asset(loc, asset_id):
                vol = str(row.get("回迁量", "0"))
                try:
                    return float(vol.replace("GB", "")) * TRANSFER_RATE_PER_GB if "GB" in vol else 15.0
                except ValueError:
                    pass
        return 3.0

    def _write_cost_report(self, asset_id: str, entry: dict[str, Any]) -> None:
        filename = f"cost_report_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "asset_id": asset_id,
            "record_id": str(entry.get("记录编号", "")),
            "storage_cost": str(entry.get("存储成本", "")),
            "compute_cost": str(entry.get("计算成本", "")),
            "transfer_cost": str(entry.get("传输成本", "")),
            "period": str(entry.get("统计周期", "")),
            "trend": str(entry.get("趋势方向", "")),
            "anomaly": str(entry.get("异常标记", "")),
            "calculated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)
