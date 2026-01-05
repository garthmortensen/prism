from ra_agent.tools.analysis_tools import compare_two_runs

run_a = "27ae1037-6601-46ca-96b7-fc5efd62cb36"
run_b = "9a4d7fd9-2b61-4fc4-8f37-8e65f5f3e1a2"

print(f"Running compare_two_runs({run_a}, {run_b})...")
result = compare_two_runs(run_a, run_b)
print("Result:")
print(result)
