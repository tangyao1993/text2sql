import logging
from typing import Dict, List, Any, Optional, Tuple
import re
from datetime import datetime, timedelta

from ..config import settings


class QueryParser:
    """Parses user queries to extract entities and intent."""
    
    def __init__(self):
        """Initialize query parser."""
        self.logger = logging.getLogger(__name__)
        
        # Predefined patterns
        self.time_patterns = {
            '今天': lambda: datetime.now().date(),
            '昨天': lambda: datetime.now().date() - timedelta(days=1),
            '本周': lambda: self._get_week_range(),
            '上周': lambda: self._get_last_week_range(),
            '本月': lambda: self._get_month_range(),
            '上月': lambda: self._get_last_month_range(),
            '今年': lambda: self._get_year_range(),
            '去年': lambda: self._get_last_year_range()
        }
        
        self.metric_patterns = [
            r'总额|总金额|总计|合计',
            r'平均值|均值|平均',
            r'最大值|最高|最大',
            r'最小值|最低|最小',
            r'数量|个数|总数',
            r'客单价|人均消费',
            r'销售额|营收|收入',
            r'利润|盈利',
            r'成本|花费'
        ]
        
        self.aggregation_patterns = {
            'sum': ['总额', '总金额', '总计', '合计', '销售额', '营收', '收入'],
            'avg': ['平均值', '均值', '平均', '客单价', '人均消费'],
            'max': ['最大值', '最高', '最大'],
            'min': ['最小值', '最低', '最小'],
            'count': ['数量', '个数', '总数']
        }
    
    def parse_query(self, query: str) -> Dict[str, Any]:
        """
        Parse user query to extract structured information.
        
        Args:
            query: User's natural language query
            
        Returns:
            Dictionary with parsed information
        """
        parsed = {
            'original_query': query,
            'entities': self._extract_entities(query),
            'time_range': self._extract_time_range(query),
            'metrics': self._extract_metrics(query),
            'dimensions': self._extract_dimensions(query),
            'filters': self._extract_filters(query),
            'intent': self._classify_intent(query),
            'aggregation_type': self._detect_aggregation(query)
        }
        
        self.logger.info(f"Parsed query: {parsed}")
        return parsed
    
    def _extract_entities(self, query: str) -> List[str]:
        """Extract entities from query."""
        entities = []
        
        # Extract potential table/column names (could be enhanced with NLP)
        words = re.findall(r'\w+', query)
        
        # Simple heuristic: longer words are more likely to be entities
        for word in words:
            if len(word) > 2 and word not in ['查询', '显示', '获取', '计算', '统计']:
                entities.append(word)
        
        return entities
    
    def _extract_time_range(self, query: str) -> Dict[str, Any]:
        """Extract time range information."""
        time_info = {
            'explicit_time': None,
            'relative_time': None,
            'start_date': None,
            'end_date': None
        }
        
        # Check for explicit time patterns
        for time_expr, time_func in self.time_patterns.items():
            if time_expr in query:
                time_info['relative_time'] = time_expr
                result = time_func()
                if isinstance(result, tuple):
                    time_info['start_date'], time_info['end_date'] = result
                else:
                    time_info['explicit_time'] = result
        
        # Check for date patterns (YYYY-MM-DD, YYYY/MM/DD, etc.)
        date_patterns = [
            r'\d{4}-\d{1,2}-\d{1,2}',
            r'\d{4}/\d{1,2}/\d{1,2}',
            r'\d{4}年\d{1,2}月\d{1,2}日'
        ]
        
        dates = []
        for pattern in date_patterns:
            dates.extend(re.findall(pattern, query))
        
        if dates:
            time_info['explicit_time'] = dates
        
        return time_info
    
    def _extract_metrics(self, query: str) -> List[str]:
        """Extract metric names from query."""
        metrics = []
        
        for pattern in self.metric_patterns:
            matches = re.findall(pattern, query)
            metrics.extend(matches)
        
        return list(set(metrics))
    
    def _extract_dimensions(self, query: str) -> List[str]:
        """Extract dimension names from query."""
        # Dimensions are typically group-by fields
        # Common patterns: "按XX", "XX的", "每个XX"
        dimension_patterns = [
            r'按(\w+)',
            r'(\w+)的',
            r'每个(\w+)',
            r'各个(\w+)'
        ]
        
        dimensions = []
        for pattern in dimension_patterns:
            matches = re.findall(pattern, query)
            dimensions.extend(matches)
        
        return list(set(dimensions))
    
    def _extract_filters(self, query: str) -> List[Dict[str, Any]]:
        """Extract filter conditions from query."""
        filters = []
        
        # Look for comparison patterns
        comparison_patterns = [
            (r'(\w+)\s*(大于|>)\s*(\d+)', 'gt'),
            (r'(\w+)\s*(小于|<)\s*(\d+)', 'lt'),
            (r'(\w+)\s*(等于|=|是)\s*(\w+)', 'eq'),
            (r'(\w+)\s*(不等于|!=|不是)\s*(\w+)', 'ne'),
            (r'(\w+)\s*(在|包含)\s*([\w,]+)', 'in'),
            (r'(\w+)\s*(不在|不包含)\s*([\w,]+)', 'not_in')
        ]
        
        for pattern, op in comparison_patterns:
            matches = re.findall(pattern, query)
            for match in matches:
                if len(match) == 3:
                    filters.append({
                        'field': match[0],
                        'operator': op,
                        'value': match[2]
                    })
        
        return filters
    
    def _classify_intent(self, query: str) -> str:
        """Classify the intent of the query."""
        query_lower = query.lower()
        
        # Classification rules
        if any(word in query_lower for word in ['统计', '计算', '多少', '几个']):
            return 'aggregation'
        elif any(word in query_lower for word in ['最高', '最大', '最低', '最小']):
            return 'extreme'
        elif any(word in query_lower for word in ['平均', '均值']):
            return 'average'
        elif any(word in query_lower for word in ['排名', '排行', 'top']):
            return 'ranking'
        elif any(word in query_lower for word in ['趋势', '变化', '增长']):
            return 'trend'
        elif any(word in query_lower for word in ['占比', '比例', '百分比']):
            return 'proportion'
        else:
            return 'simple'
    
    def _detect_aggregation(self, query: str) -> Optional[str]:
        """Detect the type of aggregation needed."""
        query_lower = query.lower()
        
        for agg_type, patterns in self.aggregation_patterns.items():
            if any(pattern in query_lower for pattern in patterns):
                return agg_type
        
        return None
    
    def _get_week_range(self) -> Tuple[str, str]:
        """Get current week date range."""
        today = datetime.now()
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        return start_of_week.date().isoformat(), end_of_week.date().isoformat()
    
    def _get_last_week_range(self) -> Tuple[str, str]:
        """Get last week date range."""
        today = datetime.now()
        start_of_last_week = today - timedelta(days=today.weekday() + 7)
        end_of_last_week = start_of_last_week + timedelta(days=6)
        return start_of_last_week.date().isoformat(), end_of_last_week.date().isoformat()
    
    def _get_month_range(self) -> Tuple[str, str]:
        """Get current month date range."""
        today = datetime.now()
        start_of_month = today.replace(day=1)
        end_of_month = today
        return start_of_month.date().isoformat(), end_of_month.date().isoformat()
    
    def _get_last_month_range(self) -> Tuple[str, str]:
        """Get last month date range."""
        today = datetime.now()
        if today.month == 1:
            last_month = today.replace(year=today.year - 1, month=12, day=1)
        else:
            last_month = today.replace(month=today.month - 1, day=1)
        
        if last_month.month == 12:
            next_month = last_month.replace(year=last_month.year + 1, month=1, day=1)
        else:
            next_month = last_month.replace(month=last_month.month + 1, day=1)
        
        end_of_last_month = next_month - timedelta(days=1)
        return last_month.date().isoformat(), end_of_last_month.date().isoformat()
    
    def _get_year_range(self) -> Tuple[str, str]:
        """Get current year date range."""
        today = datetime.now()
        start_of_year = today.replace(month=1, day=1)
        end_of_year = today
        return start_of_year.date().isoformat(), end_of_year.date().isoformat()
    
    def _get_last_year_range(self) -> Tuple[str, str]:
        """Get last year date range."""
        today = datetime.now()
        start_of_last_year = today.replace(year=today.year - 1, month=1, day=1)
        end_of_last_year = today.replace(year=today.year - 1, month=12, day=31)
        return start_of_last_year.date().isoformat(), end_of_last_year.date().isoformat()
    
    def enhance_search_query(self, original_query: str, parsed_info: Dict[str, Any]) -> str:
        """
        Enhance the search query for RAG based on parsed information.
        
        Args:
            original_query: Original user query
            parsed_info: Parsed query information
            
        Returns:
            Enhanced search query
        """
        enhanced_parts = [original_query]
        
        # Add entities
        if parsed_info['entities']:
            enhanced_parts.append(' '.join(parsed_info['entities']))
        
        # Add metrics
        if parsed_info['metrics']:
            enhanced_parts.append(' '.join(parsed_info['metrics']))
        
        # Add dimensions
        if parsed_info['dimensions']:
            enhanced_parts.append(' '.join(parsed_info['dimensions']))
        
        return ' '.join(enhanced_parts)