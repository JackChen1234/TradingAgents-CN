"""
基于MongoDB的股票筛选服务
利用本地数据库中的股票基础信息进行高效筛选
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from app.core.database import get_mongo_db
# from app.models.screening import ScreeningCondition  # 避免循环导入

logger = logging.getLogger(__name__)


class DatabaseScreeningService:
    """基于数据库的股票筛选服务"""
    
    def __init__(self):
        # 使用视图而不是基础信息表，视图已经包含了实时行情数据
        self.collection_name = "stock_screening_view"
        
        # 支持的基础信息字段映射
        self.basic_fields = {
            # 基本信息
            "code": "code",
            "name": "name", 
            "industry": "industry",
            "area": "area",
            "market": "market",
            "list_date": "list_date",
            
            # 市值信息 (亿元)
            "total_mv": "total_mv",      # 总市值
            "circ_mv": "circ_mv",        # 流通市值
            "market_cap": "total_mv",    # 市值别名

            # 财务指标
            "pe": "pe",                  # 市盈率
            "pb": "pb",                  # 市净率
            "pe_ttm": "pe_ttm",         # 滚动市盈率
            "pb_mrq": "pb_mrq",         # 最新市净率
            "roe": "roe",                # 净资产收益率（最近一期）

            # 交易指标
            "turnover_rate": "turnover_rate",  # 换手率%
            "volume_ratio": "volume_ratio",    # 量比

            # 实时行情字段（需要从 market_quotes 关联查询）
            "pct_chg": "pct_chg",              # 涨跌幅%
            "amount": "amount",                # 成交额（万元）
            "close": "close",                  # 收盘价
            "volume": "volume",                # 成交量
        }
        
        # 支持的操作符
        self.operators = {
            ">": "$gt",
            "<": "$lt", 
            ">=": "$gte",
            "<=": "$lte",
            "==": "$eq",
            "!=": "$ne",
            "between": "$between",  # 自定义处理
            "in": "$in",
            "not_in": "$nin",
            "contains": "$regex",   # 字符串包含
        }
    
    async def can_handle_conditions(self, conditions: List[Dict[str, Any]]) -> bool:
        """
        检查是否可以完全通过数据库筛选处理这些条件
        
        Args:
            conditions: 筛选条件列表
            
        Returns:
            bool: 是否可以处理
        """
        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
            
            # 检查字段是否支持
            if field not in self.basic_fields:
                logger.debug(f"字段 {field} 不支持数据库筛选")
                return False
            
            # 检查操作符是否支持
            if operator not in self.operators:
                logger.debug(f"操作符 {operator} 不支持数据库筛选")
                return False
        
        return True
    
    async def screen_stocks(
        self,
        conditions: List[Dict[str, Any]],
        limit: int = 50,
        offset: int = 0,
        order_by: Optional[List[Dict[str, str]]] = None,
        source: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        基于数据库进行股票筛选

        Args:
            conditions: 筛选条件列表
            limit: 返回数量限制
            offset: 偏移量
            order_by: 排序条件 [{"field": "total_mv", "direction": "desc"}]
            source: 数据源（可选），默认使用优先级最高的数据源

        Returns:
            Tuple[List[Dict], int]: (筛选结果, 总数量)
        """
        try:
            db = get_mongo_db()
            basic_collection = db[self.collection_name]
            quotes_collection = db["market_quotes"]

            # 区分静态基础条件（stock_screening_view）与 动态行情/估值条件（market_quotes）
            quote_metric_fields = {
                "total_mv", "circ_mv", "pe", "pb", "pe_ttm", "pb_mrq",
                "roe", "turnover_rate", "volume_ratio", "close", "pct_chg",
                "amount", "volume"
            }

            basic_conditions = []
            quote_conditions = []

            for cond in conditions:
                field = cond.get("field") if isinstance(cond, dict) else getattr(cond, "field", None)
                if field in quote_metric_fields:
                    quote_conditions.append(cond)
                else:
                    basic_conditions.append(cond)

            logger.info(f"🔍 [screen_stocks] 基础条件数: {len(basic_conditions)}, 行情/估值条件数: {len(quote_conditions)}")

            basic_query = await self._build_query(basic_conditions)
            quote_query = await self._build_query(quote_conditions)

            # 获取匹配基础条件的股票代码
            basic_codes = None
            if basic_conditions:
                basic_codes = set(await basic_collection.distinct("code", basic_query))
                logger.info(f"📋 满足基础条件的股票数: {len(basic_codes)}")

            # 获取匹配行情估值条件的股票代码
            quote_codes = None
            if quote_conditions:
                quote_codes = set(await quotes_collection.distinct("code", quote_query))
                logger.info(f"📋 满足行情/估值条件的股票数: {len(quote_codes)}")

            # 计算交集代码
            if basic_codes is not None and quote_codes is not None:
                final_codes = list(basic_codes.intersection(quote_codes))
            elif basic_codes is not None:
                final_codes = list(basic_codes)
            elif quote_codes is not None:
                final_codes = list(quote_codes)
            else:
                final_codes = None  # 代表全部股票

            # 构建最终查询
            if final_codes is not None:
                query = {"code": {"$in": final_codes}}
                total_count = len(final_codes)
            else:
                query = {}
                total_count = await basic_collection.count_documents(query)

            logger.info(f"📋 最终数据库查询匹配总数: {total_count}")

            # 构建排序条件
            sort_conditions = self._build_sort_conditions(order_by)

            # 执行查询
            cursor = basic_collection.find(query)

            # 应用排序
            if sort_conditions:
                cursor = cursor.sort(sort_conditions)

            # 应用分页
            cursor = cursor.skip(offset).limit(limit)

            # 获取结果
            results = []
            codes = []
            async for doc in cursor:
                result = self._format_result(doc)
                results.append(result)
                codes.append(doc.get("code"))

            # 批量查询财务与行情数据填充
            if codes:
                await self._enrich_with_financial_data(results, codes)

            logger.info(f"✅ 数据库筛选完成: 总数={total_count}, 返回={len(results)}")

            return results, total_count

        except Exception as e:
            logger.error(f"❌ 数据库筛选失败: {e}")
            raise Exception(f"数据库筛选失败: {str(e)}")
    
    async def _build_query(self, conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """构建MongoDB查询条件"""
        query = {}

        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
            value = condition.get("value") if isinstance(condition, dict) else condition.value

            logger.info(f"🔍 [_build_query] 处理条件: field={field}, operator={operator}, value={value}")

            # 映射字段名
            db_field = self.basic_fields.get(field)
            if not db_field:
                logger.warning(f"⚠️ [_build_query] 字段 {field} 不在 basic_fields 映射中，跳过")
                continue

            logger.info(f"✅ [_build_query] 字段映射: {field} -> {db_field}")
            
            # 处理不同操作符
            if operator == "between":
                # between操作需要两个值
                if isinstance(value, list) and len(value) == 2:
                    query[db_field] = {
                        "$gte": value[0],
                        "$lte": value[1]
                    }
            elif operator == "contains":
                # 字符串包含（不区分大小写）
                query[db_field] = {
                    "$regex": str(value),
                    "$options": "i"
                }
            elif operator in self.operators:
                # 标准操作符
                mongo_op = self.operators[operator]
                query[db_field] = {mongo_op: value}
            
        return query
    
    def _build_sort_conditions(self, order_by: Optional[List[Dict[str, str]]]) -> List[Tuple[str, int]]:
        """构建排序条件"""
        if not order_by:
            # 默认按总市值降序排序
            return [("total_mv", -1)]
        
        sort_conditions = []
        for order in order_by:
            field = order.get("field")
            direction = order.get("direction", "desc")
            
            # 映射字段名
            db_field = self.basic_fields.get(field)
            if not db_field:
                continue
            
            # 映射排序方向
            sort_direction = -1 if direction.lower() == "desc" else 1
            sort_conditions.append((db_field, sort_direction))
        
        return sort_conditions
    
    async def _enrich_with_financial_data(self, results: List[Dict[str, Any]], codes: List[str]) -> None:
        """
        批量查询行情与财务数据并填充到结果中
        """
        try:
            db = get_mongo_db()
            quotes_coll = db['market_quotes']
            financial_collection = db['stock_financial_data']

            # 1. 批量查询行情与估值指标 (market_quotes)
            quotes_cursor = quotes_coll.find({"code": {"$in": codes}}, projection={"_id": 0})
            quotes_map = {doc.get("code"): doc async for doc in quotes_cursor}

            for result in results:
                code = result.get("code")
                q = quotes_map.get(code)
                if q:
                    for f in ["close", "pct_chg", "total_mv", "circ_mv", "pe", "pb", "amount", "volume", "turnover_rate"]:
                        if q.get(f) is not None:
                            result[f] = q.get(f)

            # 2. 批量查询财报 ROE (stock_financial_data)
            pipeline = [
                {"$match": {"code": {"$in": codes}}},
                {"$sort": {"code": 1, "report_period": -1}},
                {"$group": {
                    "_id": "$code",
                    "roe": {"$first": "$roe"},
                }}
            ]

            async for doc in financial_collection.aggregate(pipeline):
                code = doc.get("_id")
                for result in results:
                    if result.get("code") == code and result.get("roe") is None:
                        result["roe"] = doc.get("roe")

            logger.debug(f"✅ 已填充 {len(results)} 条行情与财务数据")

        except Exception as e:
            logger.warning(f"⚠️ 填充财务数据失败: {e}")
            # 不抛出异常，允许继续返回基础数据

    def _format_result(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """格式化查询结果，统一使用后端字段名"""
        code_str = str(doc.get("code") or "").zfill(6)
        market_type = "A股"

        sse = doc.get("sse") or doc.get("exchange")
        board = doc.get("market") if doc.get("market") not in (None, "", "A股") else doc.get("board")

        if not sse:
            if code_str.startswith(("6", "9")):
                sse = "上海证券交易所"
            elif code_str.startswith(("0", "3")):
                sse = "深圳证券交易所"
            elif code_str.startswith(("4", "8", "920")):
                sse = "北京证券交易所"
            else:
                sse = "上海证券交易所"

        if not board:
            if code_str.startswith("688"):
                board = "科创板"
            elif code_str.startswith(("300", "301")):
                board = "创业板"
            elif code_str.startswith(("8", "4", "920")):
                board = "北交所"
            else:
                board = "主板"

        result = {
            # 基础信息
            "code": code_str,
            "name": doc.get("name") or doc.get("code"),
            "industry": doc.get("industry") or "-",
            "area": doc.get("area") or "-",
            "market": market_type,  # 市场类型（A股、美股、港股）
            "board": board,        # 板块（主板、创业板、科创板等）
            "exchange": sse,       # 交易所（上海证券交易所、深圳证券交易所等）
            "list_date": doc.get("list_date") or "",

            # 市值信息（亿元）
            "total_mv": doc.get("total_mv"),
            "circ_mv": doc.get("circ_mv"),

            # 财务指标
            "pe": doc.get("pe"),
            "pb": doc.get("pb"),
            "pe_ttm": doc.get("pe_ttm"),
            "pb_mrq": doc.get("pb_mrq"),
            "roe": doc.get("roe"),

            # 交易指标
            "turnover_rate": doc.get("turnover_rate"),
            "volume_ratio": doc.get("volume_ratio"),

            # 交易数据
            "close": doc.get("close"),              # 收盘价
            "pct_chg": doc.get("pct_chg"),          # 涨跌幅(%)
            "amount": doc.get("amount"),            # 成交额
            "volume": doc.get("volume"),            # 成交量
            "open": doc.get("open"),                # 开盘价
            "high": doc.get("high"),                # 最高价
            "low": doc.get("low"),                  # 最低价

            # 元数据
            "source": doc.get("source", "database"),
            "updated_at": doc.get("updated_at"),
        }
        
        return {k: v for k, v in result.items() if v is not None}
    
    async def get_field_statistics(self, field: str) -> Dict[str, Any]:
        """
        获取字段的统计信息
        
        Args:
            field: 字段名
            
        Returns:
            Dict: 统计信息 {min, max, avg, count}
        """
        try:
            db_field = self.basic_fields.get(field)
            if not db_field:
                return {}
            
            db = get_mongo_db()
            collection = db[self.collection_name]
            
            # 使用聚合管道获取统计信息
            pipeline = [
                {"$match": {db_field: {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": None,
                    "min": {"$min": f"${db_field}"},
                    "max": {"$max": f"${db_field}"},
                    "avg": {"$avg": f"${db_field}"},
                    "count": {"$sum": 1}
                }}
            ]
            
            result = await collection.aggregate(pipeline).to_list(length=1)
            
            if result:
                stats = result[0]
                avg_value = stats.get("avg")
                return {
                    "field": field,
                    "min": stats.get("min"),
                    "max": stats.get("max"),
                    "avg": round(avg_value, 2) if avg_value is not None else None,
                    "count": stats.get("count", 0)
                }
            
            return {"field": field, "count": 0}
            
        except Exception as e:
            logger.error(f"获取字段统计失败: {e}")
            return {"field": field, "error": str(e)}
    
    def _separate_conditions(self, conditions: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        分离基础信息条件和实时行情条件

        Args:
            conditions: 所有筛选条件

        Returns:
            Tuple[基础信息条件列表, 实时行情条件列表]
        """
        # 实时行情字段（需要从 market_quotes 查询）
        quote_fields = {"pct_chg", "amount", "close", "volume"}

        basic_conditions = []
        quote_conditions = []

        for condition in conditions:
            field = condition.get("field") if isinstance(condition, dict) else condition.field
            if field in quote_fields:
                quote_conditions.append(condition)
            else:
                basic_conditions.append(condition)

        return basic_conditions, quote_conditions

    async def _filter_by_quotes(
        self,
        results: List[Dict[str, Any]],
        codes: List[str],
        quote_conditions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        根据实时行情数据进行二次筛选

        Args:
            results: 初步筛选结果
            codes: 股票代码列表
            quote_conditions: 实时行情筛选条件

        Returns:
            List[Dict]: 筛选后的结果
        """
        try:
            db = get_mongo_db()
            quotes_collection = db['market_quotes']

            # 批量查询实时行情数据
            quotes_cursor = quotes_collection.find({"code": {"$in": codes}})
            quotes_map = {}
            async for quote in quotes_cursor:
                code = quote.get("code")
                quotes_map[code] = {
                    "close": quote.get("close"),
                    "pct_chg": quote.get("pct_chg"),
                    "amount": quote.get("amount"),
                    "volume": quote.get("volume"),
                }

            logger.info(f"📊 查询到 {len(quotes_map)} 只股票的实时行情数据")

            # 过滤结果
            filtered_results = []
            for result in results:
                code = result.get("code")
                quote_data = quotes_map.get(code)

                if not quote_data:
                    # 没有实时行情数据，跳过
                    continue

                # 检查是否满足所有实时行情条件
                match = True
                for condition in quote_conditions:
                    field = condition.get("field") if isinstance(condition, dict) else condition.field
                    operator = condition.get("operator") if isinstance(condition, dict) else condition.operator
                    value = condition.get("value") if isinstance(condition, dict) else condition.value

                    field_value = quote_data.get(field)
                    if field_value is None:
                        match = False
                        break

                    # 检查条件
                    if operator == "between" and isinstance(value, list) and len(value) == 2:
                        if not (value[0] <= field_value <= value[1]):
                            match = False
                            break
                    elif operator == ">":
                        if not (field_value > value):
                            match = False
                            break
                    elif operator == "<":
                        if not (field_value < value):
                            match = False
                            break
                    elif operator == ">=":
                        if not (field_value >= value):
                            match = False
                            break
                    elif operator == "<=":
                        if not (field_value <= value):
                            match = False
                            break

                if match:
                    # 将实时行情数据合并到结果中
                    result.update(quote_data)
                    filtered_results.append(result)

            logger.info(f"✅ 实时行情筛选完成: 筛选前={len(results)}, 筛选后={len(filtered_results)}")
            return filtered_results

        except Exception as e:
            logger.error(f"❌ 实时行情筛选失败: {e}")
            # 如果失败，返回原始结果
            return results

    async def get_available_values(self, field: str, limit: int = 100) -> List[str]:
        """
        获取字段的可选值列表（用于枚举类型字段）
        
        Args:
            field: 字段名
            limit: 返回数量限制
            
        Returns:
            List[str]: 可选值列表
        """
        try:
            db_field = self.basic_fields.get(field)
            if not db_field:
                return []
            
            db = get_mongo_db()
            collection = db[self.collection_name]
            
            # 获取字段的不重复值
            values = await collection.distinct(db_field)
            
            # 过滤None值并排序
            values = [v for v in values if v is not None]
            values.sort()
            
            return values[:limit]
            
        except Exception as e:
            logger.error(f"获取字段可选值失败: {e}")
            return []


# 全局服务实例
_database_screening_service: Optional[DatabaseScreeningService] = None


def get_database_screening_service() -> DatabaseScreeningService:
    """获取数据库筛选服务实例"""
    global _database_screening_service
    if _database_screening_service is None:
        _database_screening_service = DatabaseScreeningService()
    return _database_screening_service
