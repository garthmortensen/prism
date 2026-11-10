#!/usr/bin/env bash
# Prism Podman Quick Start Script
# This script provides interactive commands for common operations

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}"
cat << "EOF"
    ____       _                 
   / __ \_____(_)________ ___ 
  / /_/ / ___/ / ___/ __ `__ \
 / ____/ /  / (__  ) / / / / /
/_/   /_/  /_/____/_/ /_/ /_/ 
                               
Risk Adjustment Analytics Platform
Podman Quick Start
EOF
echo -e "${NC}"

show_menu() {
    echo -e "${GREEN}=== Prism Podman Operations ===${NC}"
    echo "1. Start full stack (PostgreSQL + Prism)"
    echo "2. Start PostgreSQL only"
    echo "3. Stop all services"
    echo "4. View logs"
    echo "5. Initialize database"
    echo "6. Run dbt models"
    echo "7. Open shell in Prism container"
    echo "8. Open PostgreSQL shell"
    echo "9. Rebuild images"
    echo "10. Clean up (remove all volumes)"
    echo "11. Show service status"
    echo "12. Run tests"
    echo "0. Exit"
    echo ""
}

start_full_stack() {
    echo -e "${BLUE}Starting PostgreSQL and Prism...${NC}"
    make compose-up
    echo -e "${GREEN}✓ Services started${NC}"
    echo -e "${YELLOW}Dagster UI: http://localhost:3000${NC}"
    echo -e "${YELLOW}PostgreSQL: localhost:5432${NC}"
}

start_postgres_only() {
    echo -e "${BLUE}Starting PostgreSQL only...${NC}"
    podman-compose -f infrastructure/docker-compose.yml up -d postgres
    echo -e "${GREEN}✓ PostgreSQL started${NC}"
    echo -e "${YELLOW}PostgreSQL: localhost:5432${NC}"
    echo -e "Run Prism locally with: ${BLUE}make dagster${NC}"
}

stop_services() {
    echo -e "${BLUE}Stopping all services...${NC}"
    make compose-down
    echo -e "${GREEN}✓ Services stopped${NC}"
}

view_logs() {
    echo -e "${BLUE}Following logs (Ctrl+C to exit)...${NC}"
    make compose-logs
}

init_database() {
    echo -e "${BLUE}Initializing database schemas...${NC}"
    make compose-bootstrap
    echo -e "${GREEN}✓ Database initialized${NC}"
}

run_dbt() {
    echo -e "${BLUE}Running dbt models...${NC}"
    make compose-dbt-build
    echo -e "${GREEN}✓ dbt build complete${NC}"
}

open_shell() {
    echo -e "${BLUE}Opening bash shell in Prism container...${NC}"
    make compose-shell
}

open_psql() {
    echo -e "${BLUE}Opening PostgreSQL shell...${NC}"
    make compose-postgres
}

rebuild_images() {
    echo -e "${BLUE}Rebuilding images...${NC}"
    make compose-build
    echo -e "${GREEN}✓ Images rebuilt${NC}"
}

cleanup() {
    echo -e "${RED}WARNING: This will delete all data!${NC}"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        echo -e "${BLUE}Stopping services and removing volumes...${NC}"
        podman-compose -f infrastructure/docker-compose.yml down -v
        echo -e "${GREEN}✓ Cleanup complete${NC}"
    else
        echo -e "${YELLOW}Cancelled${NC}"
    fi
}

show_status() {
    echo -e "${BLUE}Service Status:${NC}"
    podman-compose -f infrastructure/docker-compose.yml ps
    echo ""
    echo -e "${BLUE}Volumes:${NC}"
    podman volume ls | grep prism
}

run_tests() {
    echo -e "${BLUE}Running tests...${NC}"
    podman-compose -f infrastructure/docker-compose.yml exec prism uv run pytest -v
}

# Main loop
while true; do
    show_menu
    read -p "Select an option: " choice
    echo ""
    
    case $choice in
        1) start_full_stack ;;
        2) start_postgres_only ;;
        3) stop_services ;;
        4) view_logs ;;
        5) init_database ;;
        6) run_dbt ;;
        7) open_shell ;;
        8) open_psql ;;
        9) rebuild_images ;;
        10) cleanup ;;
        11) show_status ;;
        12) run_tests ;;
        0) 
            echo -e "${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            ;;
    esac
    
    echo ""
    read -p "Press Enter to continue..."
    echo ""
done
