"""
Logger configuration for Oracle Table Migration Tool.
"""
import logging
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

console = Console()

def setup_logger(name: str = "oracle_migration"):
    """
    Configure and return a logger with rich formatting.
    
    Args:
        name (str): Logger name
        
    Returns:
        logging.Logger: Configured logger
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, console=console)]
    )
    
    return logging.getLogger(name)

def create_progress_bar():
    """
    Create a rich progress bar for migration tasks.
    
    Returns:
        Progress: Rich progress bar
    """
    return Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=console
    )

logger = setup_logger()