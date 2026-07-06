"""
增强的股票筛选服务
结合数据库优化和传统筛选方式，提供高效的股票筛选功能
"""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from app.models.screening import ScreeningCondition, FieldType, BASIC_FIELDS_INFO
from app.services.database_screening_service import get_database_screening_service
from app.services.screening_service import ScreeningService, ScreeningParams

logger = logging.getLogger(__name__)

from app.services.enhanced_screening.utils import (
    analyze_conditions as _analyze_conditions_util,
    convert_conditions_to_traditional_format as _convert_to_traditional_util,
)
from app.core.database import get_mongo_db


class EnhancedScreeningService:
    """增强的股票筛选服务"""

    def __init__(self):
        self.db_service = get_database_screening_service()
        self.traditional_service = ScreeningService()

        # 支持数据库优化的字段
        self.db_supported_fields = set(BASIC_FIELDS_INFO.keys())

    async def screen_stocks(
        self,
        conditions: List[ScreeningCondition],
        market: str = "CN",
        date: Optional[str] = None,
        adj: str = "qfq",
        limit: int = 50,
        offset: int = 0,
        order_by: Optional[List[Dict[str, str]]] = None,
        use_database_optimization: bool = True
    ) -> Dict[str, Any]:
        """
        智能股票筛选

        Args:
            conditions: 筛选条件列表
            market: 市场
            date: 交易日期
            adj: 复权方式
            limit: 返回数量限制
            offset: 偏移量
            order_by: 排序条件
            use_database_optimization: 是否使用数据库优化

        Returns:
            Dict: 筛选结果
        """
        start_time = time.time()

        try:
            # 分析筛选条件
            analysis = self._analyze_conditions(conditions)

            # 决定使用哪种筛选方式
            if (use_database_optimization and
                analysis["can_use_database"] and
                not analysis["needs_technical_indicators"]):

                # 使用数据库优化筛选
                result = await self._screen_with_database(
                    conditions, limit, offset, order_by
                )
                optimization_used = "database"
                source = "mongodb"

            else:
                # 使用传统筛选方式
                result = await self._screen_with_traditional_method(
                    conditions, market, date, adj, limit, offset, order_by
                )
                optimization_used = "traditional"
                source = "api"

            # 提取 items/total
            items = result[0] if isinstance(result, tuple) else result.get("items", [])
            total = result[1] if isinstance(result, tuple) else result.get("total", 0)

            # 若使用数据库优化路径，则从数据库行情表进行富集（避免请求时外部调用）
            if source == "mongodb" and items:
                try:
                    db = get_mongo_db()
                    coll = db["market_quotes"]
                    codes = [str(it.get("code")).zfill(6) for it in items if it.get("code")]
                    if codes:
                        cursor = coll.find(
                            {"code": {"$in": codes}},
                            projection={"_id": 0, "code": 1, "close": 1, "pct_chg": 1, "amount": 1},
                        )
                        quotes_list = await cursor.to_list(length=len(codes))
                        quotes_map = {str(d.get("code")).zfill(6): d for d in quotes_list}
                        for it in items:
                            key = str(it.get("code")).zfill(6)
                            q = quotes_map.get(key)
                            if not q:
                                continue
                            if q.get("close") is not None:
                                it["close"] = q.get("close")
                            if q.get("pct_chg") is not None:
                                it["pct_chg"] = q.get("pct_chg")
                            if q.get("amount") is not None:
                                it["amount"] = q.get("amount")
                except Exception as enrich_err:
                    logger.warning(f"实时行情富集失败（已忽略）: {enrich_err}")

            # 为筛选结果添加实时PE/PB
            if items:
                try:
                    items = await self._enrich_results_with_realtime_metrics(items)
                except Exception as enrich_err:
                    logger.warning(f"实时PE/PB富集失败（已忽略）: {enrich_err}")

            # 计算耗时
            took_ms = int((time.time() - start_time) * 1000)

            # 返回结果
            return {
                "total": total,
                "items": items,
                "took_ms": took_ms,
                "optimization_used": optimization_used,
                "source": source,
                "analysis": analysis
            }

        except Exception as e:
            logger.error(f"❌ 股票筛选失败: {e}")
            took_ms = int((time.time() - start_time) * 1000)

            return {
                "total": 0,
                "items": [],
                "took_ms": took_ms,
                "optimization_used": "none",
                "source": "error",
                "error": str(e)
            }

    def _analyze_conditions(self, conditions: List[ScreeningCondition]) -> Dict[str, Any]:
        """Delegate condition analysis to utils."""
        analysis = _analyze_conditions_util(conditions)
        logger.info(f"📊 筛选条件分析: {analysis}")
        return analysis

    async def _screen_with_database(
        self,
        conditions: List[ScreeningCondition],
        limit: int,
        offset: int,
        order_by: Optional[List[Dict[str, str]]]
    ) -> Tuple[List[Dict[str, Any]], int]:
        """使用数据库优化筛选"""
        logger.info("🚀 使用数据库优化筛选")

        return await self.db_service.screen_stocks(
            conditions=conditions,
            limit=limit,
            offset=offset,
            order_by=order_by
        )

    async def _screen_with_traditional_method(
        self,
        conditions: List[ScreeningCondition],
        market: str,
        date: Optional[str],
        adj: str,
        limit: int,
        offset: int,
        order_by: Optional[List[Dict[str, str]]]
    ) -> Dict[str, Any]:
        """使用传统筛选方法"""
        logger.info("🔄 使用传统筛选方法")

        # 转换条件格式为传统服务支持的格式
        traditional_conditions = self._convert_conditions_to_traditional_format(conditions)

        # 创建筛选参数
        params = ScreeningParams(
            market=market,
            date=date,
            adj=adj,
            limit=limit,
            offset=offset,
            order_by=order_by
        )

        # 执行传统筛选
        result = self.traditional_service.run(traditional_conditions, params)

        return result

    def _convert_conditions_to_traditional_format(
        self,
        conditions: List[ScreeningCondition]
    ) -> Dict[str, Any]:
        """Delegate condition conversion to utils."""
        return _convert_to_traditional_util(conditions)

    async def _enrich_results_with_realtime_metrics(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        为筛选结果的高效富集行情与基本面指标
        1. 优先使用本地 MongoDB market_quotes 表
        2. 仍缺失最新行情的记录，通过 GTImg 批量行情接口极速补齐(50条约0.1秒)
        3. 保证板块和交易所字段正确对齐
        """
        if not items:
            return items

        logger.info(f"📊 [筛选结果富集] 开始对 {len(items)} 只股票进行数据富集...")

        # 1. 从 MongoDB market_quotes 读取
        try:
            db = get_mongo_db()
            coll = db["market_quotes"]
            codes = [str(it.get("code")).zfill(6) for it in items if it.get("code")]
            if codes:
                cursor = coll.find({"code": {"$in": codes}}, projection={"_id": 0})
                quotes_list = await cursor.to_list(length=len(codes))
                quotes_map = {str(d.get("code")).zfill(6): d for d in quotes_list}
                for it in items:
                    key = str(it.get("code")).zfill(6)
                    q = quotes_map.get(key)
                    if q:
                        for f in ["close", "pct_chg", "total_mv", "circ_mv", "pe", "pb", "amount", "volume", "turnover_rate"]:
                            if q.get(f) is not None and (it.get(f) is None or it.get(f) == 0):
                                it[f] = q.get(f)
        except Exception as e:
            logger.warning(f"⚠️ 数据库行情富集警告: {e}")

        # 2. 对仍然缺失价格或涨跌幅的股票，调用 GTImg 接口批量补齐
        missing_items = [
            it for it in items 
            if it.get("close") is None or it.get("pct_chg") is None or it.get("total_mv") is None or it.get("pe") is None
        ]
        if missing_items:
            try:
                import urllib.request
                import asyncio

                symbols = []
                for it in missing_items:
                    c = str(it.get("code")).zfill(6)
                    prefix = "sh" if c.startswith(("6", "9")) else "sz" if c.startswith(("0", "3")) else "bj"
                    symbols.append(prefix + c)

                sym_str = ",".join(symbols)
                url = f"http://qt.gtimg.cn/q={sym_str}"

                def fetch_gtimg():
                    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=3) as resp:
                        return resp.read().decode("gbk", errors="ignore")

                loop = asyncio.get_event_loop()
                content = await loop.run_in_executor(None, fetch_gtimg)

                gtimg_map = {}
                for line in content.split(";"):
                    line = line.strip()
                    if not line or "~" not in line:
                        continue
                    parts = line.split("~")
                    if len(parts) > 46:
                        raw_code = parts[2].zfill(6)
                        try:
                            gtimg_map[raw_code] = {
                                "close": float(parts[3]) if parts[3] and parts[3] != "0.00" else None,
                                "pct_chg": float(parts[32]) if parts[32] else None,
                                "amount": float(parts[37]) * 10000 if parts[37] else None,
                                "turnover_rate": float(parts[38]) if parts[38] else None,
                                "pe": float(parts[39]) if parts[39] else None,
                                "total_mv": float(parts[44] or parts[45]) if (parts[44] or parts[45]) else None,
                                "pb": float(parts[46]) if parts[46] else None,
                            }
                        except ValueError:
                            pass

                for it in items:
                    key = str(it.get("code")).zfill(6)
                    g = gtimg_map.get(key)
                    if g:
                        for k, v in g.items():
                            if v is not None and (it.get(k) is None or it.get(k) == 0):
                                it[k] = v
            except Exception as e:
                logger.warning(f"⚠️ 实时行情补齐网络请求警告: {e}")

        # 3. 板块与交易所兜底补全
        for it in items:
            c = str(it.get("code")).zfill(6)
            if not it.get("exchange") or it.get("exchange") == "-":
                if c.startswith(("6", "9")):
                    it["exchange"] = "上海证券交易所"
                elif c.startswith(("0", "3")):
                    it["exchange"] = "深圳证券交易所"
                elif c.startswith(("4", "8", "920")):
                    it["exchange"] = "北京证券交易所"
            if not it.get("board") or it.get("board") in ["-", "A股"]:
                if c.startswith("688"):
                    it["board"] = "科创板"
                elif c.startswith(("300", "301")):
                    it["board"] = "创业板"
                elif c.startswith(("8", "4", "920")):
                    it["board"] = "北交所"
                else:
                    it["board"] = "主板"

        return items

    async def get_field_info(self, field: str) -> Optional[Dict[str, Any]]:
        """
        获取字段信息

        Args:
            field: 字段名

        Returns:
            Dict: 字段信息
        """
        if field in BASIC_FIELDS_INFO:
            field_info = BASIC_FIELDS_INFO[field]

            # 获取统计信息
            stats = await self.db_service.get_field_statistics(field)

            # 获取可选值（对于枚举类型字段）
            available_values = None
            if field_info.data_type == "string":
                available_values = await self.db_service.get_available_values(field)

            return {
                "name": field_info.name,
                "display_name": field_info.display_name,
                "field_type": field_info.field_type.value,
                "data_type": field_info.data_type,
                "description": field_info.description,
                "unit": field_info.unit,
                "supported_operators": [op.value for op in field_info.supported_operators],
                "statistics": stats,
                "available_values": available_values
            }

        return None

    async def get_all_supported_fields(self) -> List[Dict[str, Any]]:
        """获取所有支持的字段信息"""
        fields = []

        for field_name in BASIC_FIELDS_INFO.keys():
            field_info = await self.get_field_info(field_name)
            if field_info:
                fields.append(field_info)

        return fields

    async def validate_conditions(self, conditions: List[ScreeningCondition]) -> Dict[str, Any]:
        """
        验证筛选条件

        Args:
            conditions: 筛选条件列表

        Returns:
            Dict: 验证结果
        """
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }

        for i, condition in enumerate(conditions):
            field = condition.field
            operator = condition.operator
            value = condition.value

            # 检查字段是否支持
            if field not in BASIC_FIELDS_INFO:
                validation_result["errors"].append(
                    f"条件 {i+1}: 不支持的字段 '{field}'"
                )
                validation_result["valid"] = False
                continue

            field_info = BASIC_FIELDS_INFO[field]

            # 检查操作符是否支持
            if operator not in [op.value for op in field_info.supported_operators]:
                validation_result["errors"].append(
                    f"条件 {i+1}: 字段 '{field}' 不支持操作符 '{operator}'"
                )
                validation_result["valid"] = False

            # 检查值的类型和范围
            if field_info.data_type == "number":
                if operator == "between":
                    if not isinstance(value, list) or len(value) != 2:
                        validation_result["errors"].append(
                            f"条件 {i+1}: between操作符需要两个数值"
                        )
                        validation_result["valid"] = False
                    elif not all(isinstance(v, (int, float)) for v in value):
                        validation_result["errors"].append(
                            f"条件 {i+1}: between操作符的值必须是数字"
                        )
                        validation_result["valid"] = False
                elif not isinstance(value, (int, float)):
                    validation_result["errors"].append(
                        f"条件 {i+1}: 数值字段 '{field}' 的值必须是数字"
                    )
                    validation_result["valid"] = False

        return validation_result


# 全局服务实例
_enhanced_screening_service: Optional[EnhancedScreeningService] = None


def get_enhanced_screening_service() -> EnhancedScreeningService:
    """获取增强筛选服务实例"""
    global _enhanced_screening_service
    if _enhanced_screening_service is None:
        _enhanced_screening_service = EnhancedScreeningService()
    return _enhanced_screening_service
