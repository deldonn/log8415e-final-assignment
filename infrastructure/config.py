"""
Configuration loader for the project.
"""
import os
from pathlib import Path
import yaml


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent


def load_config() -> dict:
    """Load configuration from settings.yaml."""
    config_path = get_project_root() / "config" / "settings.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    # Allow environment variable overrides
    if os.environ.get("AWS_REGION"):
        config["aws"]["region"] = os.environ["AWS_REGION"]
    
    if os.environ.get("AWS_KEY_PAIR"):
        config["aws"]["key_pair_name"] = os.environ["AWS_KEY_PAIR"]
    
    return config


# Singleton config instance
_config = None


def get_config() -> dict:
    """Get cached configuration."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


