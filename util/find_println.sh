#!/bin/bash

# Script to find println! statements in staged Rust files outside of tests
# Usage: ./find_println.sh [--all-modified]
# Default: Only checks files that are staged (added to git) but not yet committed
# --all-modified: Checks all modified files (staged and unstaged)
# Excludes:
# - Files in tests/ directories
# - Code inside #[cfg(test)] modules
# - Test functions marked with #[test]

set -e

# Parse command line arguments
CHECK_ALL_MODIFIED=false
if [[ "$1" == "--all-modified" ]]; then
    CHECK_ALL_MODIFIED=true
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

if [ "$CHECK_ALL_MODIFIED" = true ]; then
    echo -e "${BLUE}🔍 Searching for println! statements in all modified files outside of tests...${NC}"
    echo
    
    # Get all modified .rs files (staged and unstaged) that are not in tests/ directories
    modified_files=$(git diff --name-only HEAD | grep '\.rs$' | grep -v '/tests/' || true)
else
    echo -e "${BLUE}🔍 Searching for println! statements in staged files outside of tests...${NC}"
    echo
    
    # Get staged .rs files that are not in tests/ directories
    modified_files=$(git diff --cached --name-only --diff-filter=AM | grep '\.rs$' | grep -v '/tests/' || true)
fi

if [ -z "$modified_files" ]; then
    if [ "$CHECK_ALL_MODIFIED" = true ]; then
        echo -e "${GREEN}✅ No modified Rust files found to check${NC}"
    else
        echo -e "${GREEN}✅ No staged Rust files found to check${NC}"
    fi
    exit 0
fi

if [ "$CHECK_ALL_MODIFIED" = true ]; then
    echo -e "${BLUE}Modified files to check:${NC}"
else
    echo -e "${BLUE}Staged files to check:${NC}"
fi
for file in $modified_files; do
    echo -e "  • $file"
done
echo

rust_files="$modified_files"

total_files=0
files_with_println=0
total_println_count=0

# Track if we're inside a cfg(test) module or test function
in_test_module=0
brace_depth=0
test_module_start_depth=0

for file in $rust_files; do
    total_files=$((total_files + 1))
    file_has_println=0
    println_count=0
    
    # Reset state for each file
    in_test_module=0
    brace_depth=0
    test_module_start_depth=0
    
    line_num=0
    while IFS= read -r line || [[ -n "$line" ]]; do
        line_num=$((line_num + 1))
        
        # Count braces to track scope depth
        open_braces=$(echo "$line" | grep -o '{' | wc -l)
        close_braces=$(echo "$line" | grep -o '}' | wc -l)
        brace_depth=$((brace_depth + open_braces - close_braces))
        
        # Check if we're entering a cfg(test) module
        if echo "$line" | grep -E '^\s*#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]' > /dev/null; then
            # Look ahead to see if next non-empty, non-comment line is a module
            temp_line_num=$line_num
            found_mod=0
            while IFS= read -r next_line; do
                temp_line_num=$((temp_line_num + 1))
                # Skip empty lines and comments
                if echo "$next_line" | grep -E '^\s*(//.*)?$' > /dev/null; then
                    continue
                fi
                # Check if it's a module declaration
                if echo "$next_line" | grep -E '^\s*mod\s+' > /dev/null; then
                    found_mod=1
                fi
                break
            done < <(tail -n +$((line_num + 1)) "$file")
            
            if [ $found_mod -eq 1 ]; then
                in_test_module=1
                test_module_start_depth=$brace_depth
            fi
        fi
        
        # Check if we're exiting a cfg(test) module
        if [ $in_test_module -eq 1 ] && [ $brace_depth -lt $test_module_start_depth ]; then
            in_test_module=0
        fi
        
        # Skip if we're in a test module
        if [ $in_test_module -eq 1 ]; then
            continue
        fi
        
        # Skip lines that are test functions (simple heuristic)
        if echo "$line" | grep -E '^\s*#\s*\[\s*test\s*\]' > /dev/null; then
            # Skip the next few lines until we find the function and its body
            continue
        fi
        
        # Check for println! (but not in comments)
        if echo "$line" | grep -E '^\s*[^/]*println!' > /dev/null; then
            # Make sure it's not in a comment
            if ! echo "$line" | grep -E '^\s*//' > /dev/null; then
                if [ $file_has_println -eq 0 ]; then
                    echo -e "${YELLOW}📁 $file${NC}"
                    file_has_println=1
                    files_with_println=$((files_with_println + 1))
                fi
                echo -e "  ${RED}Line $line_num:${NC} $(echo "$line" | sed 's/^[[:space:]]*//')"
                println_count=$((println_count + 1))
                total_println_count=$((total_println_count + 1))
            fi
        fi
        
    done < "$file"
    
    if [ $file_has_println -eq 1 ]; then
        echo -e "  ${GREEN}Found $println_count println! statement(s)${NC}"
        echo
    fi
done

# Summary
echo -e "${BLUE}📊 Summary:${NC}"
if [ "$CHECK_ALL_MODIFIED" = true ]; then
    echo -e "  • Modified files scanned: ${total_files}"
else
    echo -e "  • Staged files scanned: ${total_files}"
fi
echo -e "  • Files with println!: ${files_with_println}"
echo -e "  • Total println! statements: ${total_println_count}"

if [ $total_println_count -eq 0 ]; then
    echo -e "${GREEN}✅ No println! statements found outside of tests!${NC}"
    exit 0
else
    echo -e "${RED}⚠️  Found println! statements that should be reviewed${NC}"
    exit 1
fi