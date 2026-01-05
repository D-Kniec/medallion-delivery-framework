.PHONY: help setup

help:
	@echo "Pizza Project - Available commands:"
	@echo "  make setup    - Create virtual environment and install dependencies"

setup:
	python3 -m venv .venv
	./.venv/bin/pip install -r requirements.txt
	@echo "Setup complete. Virtual environment ready."