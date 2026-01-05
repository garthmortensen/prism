import typer
import uuid
from rich.console import Console
from ra_agent.agent import create_agent

app = typer.Typer()
console = Console()

@app.command()
def chat():
    """Start an interactive chat session with the agent."""
    agent = create_agent()
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    console.print(f"[bold]Prism Agent[/bold] (Session: {thread_id})")
    console.print("Type 'quit' or 'exit' to end session.")
    
    while True:
        try:
            user_input = console.input("\n[bold red]You:[/bold red] ")
            if user_input.lower() in ['quit', 'exit']:
                break
            
            # Stream the response
            console.print("\n[bold red]Agent:[/bold red] ", end="")
            
            # We use invoke for now, but could switch to stream
            response = agent.invoke({"messages": [("human", user_input)]}, config)
            console.print(response['messages'][-1].content)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            console.print(f"\n[bold red]Error:[/bold red] {e}")

@app.command()
def ask(question: str):
    """Ask a single question to the agent."""
    agent = create_agent()
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    try:
        response = agent.invoke({"messages": [("human", question)]}, config)
        console.print(response['messages'][-1].content)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")

if __name__ == "__main__":
    app()
