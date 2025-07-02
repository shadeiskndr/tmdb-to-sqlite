#!/bin/bash
# Virtual environment setup script for Fedora 42

# Create virtual environment
python3 -m venv movie_data_env

# Activate virtual environment
source movie_data_env/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install required packages (sqlite3 is built into Python)
pip install pandas

echo "Virtual environment 'movie_data_env' created and activated!"
echo "Required packages installed: pandas"
echo "Note: sqlite3 is built into Python and doesn't need separate installation"
echo ""
echo "To activate this environment in the future, run:"
echo "source movie_data_env/bin/activate"
