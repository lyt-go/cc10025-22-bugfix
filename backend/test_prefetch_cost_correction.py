"""回迁费用更正分录集成测试：部分编号匹配错账的修复、核对与幂等。

场景（对应缺陷报告）：
    1. 旧逻辑用冷存位置的部分编号匹配，把 s3://cold/ASSE-1234 的 100GB 传输费
       错记到 ASSE-123 并结入月报（已发送给负责人）。
    2. 回迁入口启动长号任务，成本追踪核对任务来源后生成回迁费用更正分录。
    3. 原月报按当时口径保留可回查；本期付款汇总与两个资产的有效余额只让长号承担 9.00。
    4. 重新启动长号任务并确认核算：并发/重复核算幂等，付款结果不得变化。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.services import service_for
from app.services.base import asset_id_from_location, location_matches_asset
from app.store import store

DATA_ROOT = os.path.join(os.path.dirname(__file__), "app", "data")
RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"

SHORT = "ASSE-123"                      # 短号：被错记的资产
LONG = "ASSE-1234"                      # 长号：回迁任务的实际资产
LOCATION = f"s3://cold/{LONG}"          # 回迁入口的冷存位置
VOLUME = "100GB"
FEE = 9.00                              # 100GB × 0.09 元/GB

passed = 0
failed = 0


def check(desc, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  {GREEN}PASS{RESET} {desc}")
        passed += 1
    else:
        print(f"  {RED}FAIL{RESET} {desc} {detail}")
        failed += 1


def next_period(period):
    year, month = int(period[:4]), int(period[5:7]) + 1
    if month > 12:
        year, month = year + 1, 1
    return f"{year:04d}-{month:02d}"


def cost_row(asset_id):
    for row in store.rows("cost_tracker"):
        if row.get("关联资产") == asset_id:
            return row
    return None


def ensure_cost_row(asset_id):
    row = cost_row(asset_id)
    if row is not None:
        return row
    svc = service_for("cost_tracker")
    entry, missing = svc.create_entry({
        "记录编号": f"COST-{asset_id}",
        "关联资产": asset_id,
        "存储成本": "0.00",
        "计算成本": "0.00",
        "传输成本": "0.00",
        "统计周期": "",
        "趋势方向": "持平",
        "异常标记": "无",
    })
    assert entry is not None, f"成本记录创建失败: {missing}"
    return entry


def ensure_prefetch_task():
    for row in store.rows("hot_prefetch"):
        if row.get("冷存位置") == LOCATION:
            return row
    svc = service_for("hot_prefetch")
    entry, missing = svc.create_entry({
        "任务编号": "HOT_-9001",
        "冷存位置": LOCATION,
        "目标分区": f"{LONG}_hot",
        "回迁量": VOLUME,
        "预计耗时": "2h",
        "优先级": "高",
        "过期策略": "T+7",
        "任务状态": "排队中",
    })
    assert entry is not None, f"回迁任务创建失败: {missing}"
    return entry


def test_exact_location_matching():
    """冷存位置按完整路径段精确匹配，部分编号不再错位。"""
    print("\n=== 测试1: 冷存位置精确匹配 ===")
    check("长号命中自己的冷存位置", location_matches_asset(LOCATION, LONG))
    check("短号不再被部分编号误配", not location_matches_asset(LOCATION, SHORT))
    check("位置末段解析为归属资产", asset_id_from_location(LOCATION) == LONG)


def test_mis_settlement_then_correction():
    """错结短号月报 → 启动长号任务 → 更正分录 → 核对月报/付款汇总/余额 → 幂等重启。"""
    print("\n=== 测试2: 错账结入短号月报后启动长号任务生成更正分录 ===")
    cost_svc = service_for("cost_tracker")
    hot_svc = service_for("hot_prefetch")
    ensure_cost_row(SHORT)
    ensure_cost_row(LONG)
    task = ensure_prefetch_task()

    # ---- 1) 历史错账：100GB 传输费结入短号月报（旧的部分编号匹配造成） ----
    cost_svc.book_transfer_charge(
        SHORT, task_id=task["任务编号"], cold_location=LOCATION, volume=VOLUME)
    report, msg = cost_svc.settle_monthly_report(SHORT)
    period = report["期间"] if report else ""
    check("短号月报已结算并发送", report is not None and report["状态"] == "已发送", msg)
    check("短号月报传输费 9.00", report is not None and report["传输成本"] == FEE,
          f"实际 {report and report['传输成本']}")

    # 月结幂等：同一资产同一期间不会结出第二张月报
    again, _ = cost_svc.settle_monthly_report(SHORT)
    check("月结幂等（同一期间只有一张月报）",
          again is not None and again["月报编号"] == report["月报编号"]
          and len(cost_svc.list_monthly_reports(SHORT)) == 1)

    # ---- 2) 启动长号回迁任务，提交成本追踪更正 ----
    entry, msg = hot_svc.start_prefetch(task["id"])
    check("长号回迁任务启动", entry is not None and entry["status"] == "回迁中", msg)
    corrections = cost_svc.list_corrections()
    check("生成且只生成一笔回迁费用更正分录", len(corrections) == 1,
          f"实际 {len(corrections)} 笔")
    corr = corrections[0] if corrections else {}
    check("更正方向：短号 → 长号",
          corr.get("原资产") == SHORT and corr.get("实际资产") == LONG)
    check("更正金额 9.00", corr.get("金额") == FEE, f"实际 {corr.get('金额')}")
    check("更正计入下一期付款汇总", corr.get("生效期间") == next_period(period),
          f"实际 {corr.get('生效期间')}")
    check("修订依据可回查（任务来源）",
          corr.get("依据", {}).get("任务编号") == task["任务编号"]
          and corr.get("依据", {}).get("冷存位置") == LOCATION)

    # ---- 3) 核对：原月报 / 本期付款汇总 / 两个资产余额 ----
    original = cost_svc.get_monthly_report(SHORT, period)
    check("原月报按当时口径保留（短号 9.00 已发送）",
          original is not None and original["传输成本"] == FEE
          and original["状态"] == "已发送")
    original_summary = cost_svc.get_payment_summary(period)
    check("原期间付款汇总保持当时口径（短号 9.00）",
          original_summary["items"].get(SHORT, {}).get("应付") == FEE)

    summary = cost_svc.get_payment_summary(corr["生效期间"])
    items = summary["items"]
    check("本期付款汇总只有长号承担 9.00",
          items.get(LONG, {}).get("应付") == FEE,
          f"实际 {items.get(LONG)}")
    check("短号在本期被冲销 -9.00", items.get(SHORT, {}).get("应付") == -FEE,
          f"实际 {items.get(SHORT)}")
    check("本期汇总不涉及其他资产", len(items) == 2, f"实际 {sorted(items)}")
    check("更正在资产间轧差为零", summary["合计应付"] == 0.0,
          f"实际 {summary['合计应付']}")

    bal_short = cost_svc.get_asset_balance(SHORT)
    bal_long = cost_svc.get_asset_balance(LONG)
    check("短号有效余额不残留错账", bal_short["有效余额"] == 0.0,
          f"实际 {bal_short}")
    check("长号有效余额 9.00", bal_long["有效余额"] == FEE, f"实际 {bal_long}")
    check("短号传输指标重算为 0.00", cost_row(SHORT)["传输成本"] == "0.00",
          f"实际 {cost_row(SHORT)['传输成本']}")
    check("长号传输指标重算为 9.00", cost_row(LONG)["传输成本"] == "9.00",
          f"实际 {cost_row(LONG)['传输成本']}")

    # 原短号账与修订依据均持久化，可回查
    monthly_file = os.path.join(DATA_ROOT, "assets", f"cost_monthly_{SHORT}_{period}.json")
    correction_file = os.path.join(DATA_ROOT, "assets", f"cost_correction_{corr['更正编号']}.json")
    check("原月报文件已归档可回查", os.path.exists(monthly_file), monthly_file)
    check("更正分录文件已归档可回查", os.path.exists(correction_file), correction_file)

    # ---- 4) 重新启动长号任务并确认核算：付款结果不得变化 ----
    print("\n=== 测试3: 重复/并发启动与重复核算幂等 ===")
    hot_svc.start_prefetch(task["id"])   # 重新启动
    hot_svc.start_prefetch(task["id"])   # 模拟并发启动
    check("重复/并发启动不再生成更正分录", len(cost_svc.list_corrections()) == 1)
    check("物理传输只算一次（长号未结费用不重复）",
          cost_svc.get_asset_balance(LONG)["有效余额"] == FEE)

    calc, msg = cost_svc.calculate_cost(cost_row(LONG)["id"])
    check("长号确认核算成功", calc is not None, msg)
    check("重复核算后长号传输成本仍为 9.00", cost_row(LONG)["传输成本"] == "9.00",
          f"实际 {cost_row(LONG)['传输成本']}")
    calc2, msg = cost_svc.calculate_cost(cost_row(SHORT)["id"])
    check("短号重复核算成功", calc2 is not None, msg)
    check("短号重复核算后传输成本仍为 0.00", cost_row(SHORT)["传输成本"] == "0.00",
          f"实际 {cost_row(SHORT)['传输成本']}")

    summary2 = cost_svc.get_payment_summary(corr["生效期间"])
    check("付款汇总结果不变", summary2 == summary)
    check("短号有效余额仍无错账残留", cost_svc.get_asset_balance(SHORT)["有效余额"] == 0.0)
    check("长号有效余额仍为 9.00", cost_svc.get_asset_balance(LONG)["有效余额"] == FEE)

    # 无关回迁任务不得触发更正
    seed_task = store.rows("hot_prefetch")[0]
    hot_svc.start_prefetch(seed_task["id"])
    check("无关任务不触发更正", len(cost_svc.list_corrections()) == 1)


if __name__ == "__main__":
    print("=" * 60)
    print("回迁费用更正分录集成测试")
    print("=" * 60)

    test_exact_location_matching()
    test_mis_settlement_then_correction()

    print(f"\n{'=' * 60}")
    print(f"结果: {GREEN}{passed} 通过{RESET} / {RED}{failed} 失败{RESET} / "
          f"{passed + failed} 总计")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)
