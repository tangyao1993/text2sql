"""
Command Line Interface for Text2SQL.
"""

import click
import json
import logging
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax

from .text2sql import Text2SQL
from .config import settings


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Rich console
console = Console()


@click.group()
@click.option('--db-url', envvar='DATABASE_URL', help='Database connection URL')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
def cli(ctx, db_url, verbose):
    """Text2SQL: Convert natural language to SQL queries using RAG."""
    ctx.ensure_object(dict)
    ctx.obj['db_url'] = db_url
    ctx.obj['verbose'] = verbose
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


@cli.command()
@click.option('--force', is_flag=True, help='Force rebuild even if exists')
@click.option('--business-rules', type=click.Path(exists=True), help='JSON file with business rules')
@click.pass_context
def build(ctx, force, business_rules):
    """Build knowledge base from database metadata."""
    console.print("[bold green]Building knowledge base...[/bold green]")
    
    # Load business rules if provided
    rules = None
    if business_rules:
        with open(business_rules, 'r', encoding='utf-8') as f:
            rules = json.load(f)
    
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        text2sql.build_knowledge_base(business_rules=rules, force_rebuild=force)
        console.print("[bold green]✓[/bold green] Knowledge base built successfully!")
        
        # Show stats
        stats = text2sql.get_stats()
        console.print(f"\nKnowledge base contains {stats['knowledge_base_size']} tables")
        
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error building knowledge base: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument('query')
@click.option('--show-intermediate', is_flag=True, help='Show intermediate results')
@click.option('--max-corrections', type=int, default=3, help='Maximum correction attempts')
@click.pass_context
def query(ctx, query, show_intermediate, max_corrections):
    """Convert natural language query to SQL."""
    console.print(f"[bold blue]Query:[/bold blue] {query}")
    
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        # Convert query to SQL
        result = text2sql.query_to_sql(
            query=query,
            max_correction_attempts=max_corrections,
            return_intermediate=show_intermediate
        )
        
        # Display results
        if result['is_valid']:
            console.print("\n[bold green]✓[/bold green] Generated SQL:")
            sql_syntax = Syntax(result['sql'], "sql", theme="monokai", line_numbers=True)
            console.print(sql_syntax)
            
            if result['execution_results']:
                console.print("\n[bold green]✓[/bold green] Execution Results:")
                
                # Create table for results
                table = Table(show_header=True, header_style="bold magenta")
                if result['execution_results']:
                    for col in result['execution_results'][0].keys():
                        table.add_column(col)
                    
                    for row in result['execution_results']:
                        table.add_row(*[str(val) for val in row.values()])
                    
                    console.print(table)
            
            if result['correction_attempts'] > 0:
                console.print(f"\n[yellow]Note:[/yellow] SQL was corrected after {result['correction_attempts']} attempt(s)")
            
        else:
            console.print(f"\n[bold red]✗[/bold red] Failed to generate valid SQL")
            if result.get('error'):
                console.print(f"[red]Error:[/red] {result['error']}")
        
        # Show intermediate results if requested
        if show_intermediate and result.get('intermediate'):
            console.print("\n[bold cyan]Intermediate Results:[/bold cyan]")
            
            # Retrieved tables
            if 'retrieved_context' in result['intermediate']:
                tables = result['intermediate']['retrieved_context']['tables']
                console.print(f"Retrieved tables: {', '.join(tables)}")
                
                if result['intermediate']['retrieved_context']['relationships']:
                    console.print("Table relationships detected")
            
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error processing query: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument('sql')
@click.pass_context
def validate(ctx, sql):
    """Validate SQL query."""
    console.print("[bold blue]Validating SQL...[/bold blue]")
    
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        # Validate syntax
        is_valid, error = text2sql.sql_validator.validate_syntax(sql)
        
        if is_valid:
            console.print("[bold green]✓[/bold green] SQL syntax is valid")
        else:
            console.print(f"[bold red]✗[/bold red] Syntax error: {error}")
            return
        
        # Get execution plan
        plan = text2sql.explain_sql(sql)
        if plan:
            console.print("\n[bold cyan]Execution Plan:[/bold cyan]")
            console.print(json.dumps(plan, indent=2))
        
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error validating SQL: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.option('--table-name', help='Specific table name')
@click.pass_context
def schema(ctx, table_name):
    """Show database schema information."""
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        if table_name:
            # Show specific table
            schema_info = text2sql.get_schema_info(table_name)
            if schema_info:
                console.print(Panel(schema_info['content'], title=f"Table: {table_name}"))
            else:
                console.print(f"[yellow]Table '{table_name}' not found in knowledge base[/yellow]")
        else:
            # Show all tables
            schema_info = text2sql.get_schema_info()
            
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Table Name")
            table.add_column("Columns", justify="right")
            table.add_column("Row Count", justify="right")
            
            for t in schema_info['tables']:
                table.add_row(t['name'], str(t['columns']), str(t['row_count']))
            
            console.print(table)
            
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error getting schema: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument('rule_name')
@click.argument('rule_definition')
@click.pass_context
def add_rule(ctx, rule_name, rule_definition):
    """Add a business rule to the knowledge base."""
    console.print(f"[bold blue]Adding business rule: {rule_name}[/bold blue]")
    
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        text2sql.add_business_rule(rule_name, rule_definition)
        console.print("[bold green]✓[/bold green] Business rule added successfully!")
        
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error adding business rule: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument('output_file')
@click.pass_context
def export(ctx, output_file):
    """Export knowledge base to file."""
    console.print(f"[bold blue]Exporting knowledge base to {output_file}...[/bold blue]")
    
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        text2sql.export_knowledge_base(output_file)
        console.print("[bold green]✓[/bold green] Knowledge base exported successfully!")
        
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error exporting knowledge base: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.argument('input_file')
@click.option('--force', is_flag=True, help='Overwrite existing knowledge base')
@click.pass_context
def import_kb(ctx, input_file, force):
    """Import knowledge base from file."""
    if not force:
        console.print("[yellow]Warning: This will overwrite the existing knowledge base[/yellow]")
        if not click.confirm('Continue?'):
            return
    
    console.print(f"[bold blue]Importing knowledge base from {input_file}...[/bold blue]")
    
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        text2sql.import_knowledge_base(input_file)
        console.print("[bold green]✓[/bold green] Knowledge base imported successfully!")
        
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error importing knowledge base: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.pass_context
def stats(ctx):
    """Show system statistics."""
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    try:
        stats = text2sql.get_stats()
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Knowledge Base Size", str(stats['knowledge_base_size']))
        table.add_row("Database Type", stats['database_type'])
        table.add_row("LLM Model", stats['llm_model'])
        table.add_row("Embedding Model", stats['embedding_model_name'])
        table.add_row("Last Updated", stats['last_updated'])
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Error getting stats: {e}")
        raise click.ClickException(str(e))


@cli.command()
@click.pass_context
def interactive(ctx):
    """Start interactive mode."""
    console.print("[bold green]Welcome to Text2SQL Interactive Mode![/bold green]")
    console.print("Type 'exit' or 'quit' to exit\n")
    
    # Initialize Text2SQL
    text2sql = Text2SQL(database_url=ctx.obj['db_url'])
    
    while True:
        try:
            # Get user input
            query = click.prompt("\n[yellow]Enter your query[/yellow]", type=str)
            
            if query.lower() in ['exit', 'quit']:
                console.print("[bold green]Goodbye![/bold green]")
                break
            
            if not query:
                continue
            
            # Process query
            result = text2sql.query_to_sql(query)
            
            # Display results
            if result['is_valid']:
                console.print("\n[bold green]Generated SQL:[/bold green]")
                console.print(result['sql'])
                
                if result['execution_results']:
                    console.print("\n[bold green]Results:[/bold green]")
                    # Simple table display
                    for i, row in enumerate(result['execution_results']):
                        if i == 0:
                            console.print(" | ".join(row.keys()))
                        console.print(" | ".join(str(v) for v in row.values()))
                
                if result['correction_attempts'] > 0:
                    console.print(f"\n[dim]Corrected after {result['correction_attempts']} attempts[/dim]")
            else:
                console.print(f"\n[bold red]Error: {result.get('error', 'Unknown error')}[/bold red]")
        
        except KeyboardInterrupt:
            console.print("\n[bold green]Goodbye![/bold green]")
            break
        except Exception as e:
            console.print(f"\n[bold red]Error: {e}[/bold red]")


def main():
    """Entry point for the CLI."""
    cli()