"""
Example usage of Text2SQL system.
"""

from main import Text2SQL
import json


def main():
    """Demonstrate Text2SQL usage."""
    
    # Initialize Text2SQL
    # Make sure to set up your database connection in .env file
    text2sql = Text2SQL()
    
    print("=== Text2SQL Example Usage ===\n")
    
    # 1. Build knowledge base
    print("1. Building knowledge base...")
    try:
        text2sql.build_knowledge_base()
        print("✓ Knowledge base built successfully\n")
    except Exception as e:
        print(f"✗ Error: {e}")
        print("Note: Make sure your database is configured in .env\n")
    
    # 2. Show schema information
    print("2. Database schema:")
    try:
        schema_info = text2sql.get_schema_info()
        if schema_info and 'tables' in schema_info:
            print(f"Found {len(schema_info['tables'])} tables:")
            for table in schema_info['tables'][:5]:  # Show first 5
                print(f"  - {table['name']}: {table['columns']} columns, ~{table['row_count']} rows")
        print()
    except Exception as e:
        print(f"✗ Error getting schema: {e}\n")
    
    # 3. Add business rules
    print("3. Adding business rules...")
    try:
        text2sql.add_business_rule("客单价", "SUM(payment_amount) / COUNT(DISTINCT user_id)")
        text2sql.add_business_rule("GMV", "SUM(payment_amount)")
        print("✓ Business rules added\n")
    except Exception as e:
        print(f"✗ Error: {e}\n")
    
    # 4. Query examples
    queries = [
        "上周客单价最高的门店是哪个？",
        "统计每个城市的用户数量",
        "查询订单金额最高的前10个用户",
        "本月新增用户数",
        "各产品类别的销售额占比"
    ]
    
    print("4. Query examples:")
    for query in queries:
        print(f"\nQuery: {query}")
        try:
            result = text2sql.query_to_sql(query)
            
            if result['is_valid']:
                print(f"SQL: {result['sql']}")
                
                if result['execution_results']:
                    print("Results:")
                    # Simple display of first few results
                    for i, row in enumerate(result['execution_results'][:3]):
                        print(f"  {i+1}. {row}")
                    
                    if len(result['execution_results']) > 3:
                        print(f"  ... and {len(result['execution_results']) - 3} more rows")
                
                if result['correction_attempts'] > 0:
                    print(f"(Corrected after {result['correction_attempts']} attempts)")
            else:
                print(f"✗ Error: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"✗ Error: {e}")
    
    print("\n=== End of Examples ===")


if __name__ == "__main__":
    main()