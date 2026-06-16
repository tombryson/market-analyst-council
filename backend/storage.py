"""JSON-based storage for conversations."""

import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path
from pydantic import BaseModel, Field
from .config import DATA_DIR


# ---------------------------------------------------------------------------
# Pydantic models — validate on write so structural errors fail at insertion
# time rather than silently corrupting JSON and surfacing at render time.
# ---------------------------------------------------------------------------

class LoadingState(BaseModel):
    search: bool = False
    evidence: bool = False
    attachments: bool = False
    stage1: bool = False
    stage2: bool = False
    stage3: bool = False
    stage1Progress: int = 0
    stage1Completed: int = 0
    stage1Total: int = 0
    stage1Model: str = ""
    stage1Message: str = ""


class AssistantMessage(BaseModel):
    role: str = "assistant"
    status: str = "complete"
    stage1: Optional[Any] = None
    stage2: Optional[Any] = None
    stage3: Optional[Any] = None
    search_results: Optional[Any] = None
    evidence_pack: Optional[Any] = None
    attachments_processed: Optional[Any] = None
    loading: LoadingState = Field(default_factory=LoadingState)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Conversation(BaseModel):
    id: str
    created_at: str
    title: str = "New Conversation"
    messages: List[Any] = Field(default_factory=list)


def ensure_data_dir():
    """Ensure the data directory exists."""
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)


def get_conversation_path(conversation_id: str) -> str:
    """Get the file path for a conversation."""
    return os.path.join(DATA_DIR, f"{conversation_id}.json")


def create_conversation(conversation_id: str) -> Dict[str, Any]:
    """
    Create a new conversation.

    Args:
        conversation_id: Unique identifier for the conversation

    Returns:
        New conversation dict
    """
    ensure_data_dir()

    conversation = {
        "id": conversation_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "title": "New Conversation",
        "messages": []
    }

    # Save to file
    path = get_conversation_path(conversation_id)
    with open(path, 'w') as f:
        json.dump(conversation, f, indent=2)

    return conversation


def get_conversation(conversation_id: str) -> Optional[Dict[str, Any]]:
    """
    Load a conversation from storage.

    Args:
        conversation_id: Unique identifier for the conversation

    Returns:
        Conversation dict or None if not found
    """
    path = get_conversation_path(conversation_id)

    if not os.path.exists(path):
        return None

    with open(path, 'r') as f:
        return json.load(f)


def save_conversation(conversation: Dict[str, Any]):
    """
    Save a conversation to storage.

    Args:
        conversation: Conversation dict to save
    """
    ensure_data_dir()

    path = get_conversation_path(conversation['id'])
    with open(path, 'w') as f:
        json.dump(conversation, f, indent=2)


def _default_loading_state() -> Dict[str, Any]:
    return {
        "search": False,
        "evidence": False,
        "attachments": False,
        "stage1": False,
        "stage2": False,
        "stage3": False,
        "stage1Progress": 0,
        "stage1Completed": 0,
        "stage1Total": 0,
        "stage1Model": "",
        "stage1Message": "",
    }


def _deep_merge_dict(target: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge_dict(target[key], value)
        else:
            target[key] = value
    return target


def list_conversations() -> List[Dict[str, Any]]:
    """
    List all conversations (metadata only).

    Returns:
        List of conversation metadata dicts
    """
    ensure_data_dir()

    conversations = []
    for filename in os.listdir(DATA_DIR):
        if filename.endswith('.json'):
            path = os.path.join(DATA_DIR, filename)
            with open(path, 'r') as f:
                data = json.load(f)
                # Return metadata only
                conversations.append({
                    "id": data["id"],
                    "created_at": data["created_at"],
                    "title": data.get("title", "New Conversation"),
                    "message_count": len(data["messages"])
                })

    # Sort by creation time, newest first
    conversations.sort(key=lambda x: x["created_at"], reverse=True)

    return conversations


def add_user_message(conversation_id: str, content: str):
    """
    Add a user message to a conversation.

    Args:
        conversation_id: Conversation identifier
        content: User message content
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    conversation["messages"].append({
        "role": "user",
        "content": content
    })

    save_conversation(conversation)


def update_conversation_title(conversation_id: str, title: str):
    """
    Update the title of a conversation.

    Args:
        conversation_id: Conversation identifier
        title: New title for the conversation
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    conversation["title"] = title
    save_conversation(conversation)


def add_user_message_with_metadata(
    conversation_id: str,
    content: str,
    enable_search: bool,
    attachments: List[Dict[str, Any]],
    council_mode: str = "local",
    template_id: Optional[str] = None,
    company_name: Optional[str] = None,
    company_type: Optional[str] = None,
    exchange: Optional[str] = None,
    template_selection_source: Optional[str] = None,
    exchange_selection_source: Optional[str] = None,
):
    """
    Add a user message with search toggle and attachment metadata.

    Args:
        conversation_id: Conversation identifier
        content: User message content
        enable_search: Whether search was enabled
        attachments: List of attachment metadata dicts
        council_mode: local or perplexity_emulated
        template_id: Selected template id (optional)
        company_name: Selected company name (optional)
        company_type: Selected company type id (optional)
        exchange: Selected exchange id (optional)
        template_selection_source: Selection source metadata (optional)
        exchange_selection_source: Exchange selection source metadata (optional)
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    message = {
        "role": "user",
        "content": content
    }

    # Only store if not default (default is True)
    if not enable_search:
        message["enable_search"] = False

    # Only store if attachments present
    if attachments:
        message["attachments"] = attachments

    # Only store if non-default
    if council_mode and council_mode != "local":
        message["council_mode"] = council_mode

    if template_id:
        message["template_id"] = template_id

    if company_name:
        message["company_name"] = company_name

    if company_type:
        message["company_type"] = company_type

    if exchange:
        message["exchange"] = exchange

    if template_selection_source:
        message["template_selection_source"] = template_selection_source

    if exchange_selection_source:
        message["exchange_selection_source"] = exchange_selection_source

    conversation["messages"].append(message)
    save_conversation(conversation)


def add_assistant_placeholder_message(
    conversation_id: str,
    metadata: Optional[Dict[str, Any]] = None,
):
    """
    Persist a placeholder assistant message immediately so in-flight runs survive refreshes.
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    loading = LoadingState(stage1=True, stage1Message="Preparing analysis...")
    validated = AssistantMessage(
        status="running",
        metadata=dict(metadata or {}),
        loading=loading,
    )
    message = validated.model_dump()

    conversation["messages"].append(message)
    save_conversation(conversation)


def update_last_assistant_message(
    conversation_id: str,
    patch: Dict[str, Any],
):
    """
    Merge updates into the most recent assistant message.
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    for index in range(len(conversation["messages"]) - 1, -1, -1):
        message = conversation["messages"][index]
        if message.get("role") != "assistant":
            continue
        _deep_merge_dict(message, dict(patch or {}))
        conversation["messages"][index] = message
        save_conversation(conversation)
        return

    raise ValueError(f"No assistant message found for conversation {conversation_id}")


def add_assistant_message_with_metadata(
    conversation_id: str,
    stage1: List[Dict[str, Any]],
    stage2: List[Dict[str, Any]],
    stage3: Dict[str, Any],
    search_results: Optional[Dict[str, Any]],
    attachments_processed: List[Dict[str, Any]]
):
    """
    Add an assistant message with all 3 stages plus search/attachment metadata.

    Args:
        conversation_id: Conversation identifier
        stage1: List of individual model responses
        stage2: List of model rankings
        stage3: Final synthesized response
        search_results: Search results dict (optional)
        attachments_processed: List of processed attachment results
    """
    conversation = get_conversation(conversation_id)
    if conversation is None:
        raise ValueError(f"Conversation {conversation_id} not found")

    existing_metadata: Dict[str, Any] = {}
    if conversation["messages"] and conversation["messages"][-1].get("role") == "assistant":
        existing_metadata = dict(conversation["messages"][-1].get("metadata") or {})

    validated = AssistantMessage(
        status="complete",
        stage1=stage1,
        stage2=stage2,
        stage3=stage3,
        search_results=search_results or None,
        attachments_processed=attachments_processed or None,
        metadata=existing_metadata,
    )
    message = validated.model_dump()

    if conversation["messages"] and conversation["messages"][-1].get("role") == "assistant":
        conversation["messages"][-1] = message
    else:
        conversation["messages"].append(message)
    save_conversation(conversation)
