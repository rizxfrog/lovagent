"""
导出 LangGraph 节点流程图到 mermaid 文件
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# 导出目录
AGENT_NODE_GRAPHS_DIR = Path(__file__).resolve().parents[2] / "data" / "agent_node_graphs"


def _get_graph_mermaid(graph: Any, name: str) -> str:
    """获取单个 graph 的 mermaid 表示"""
    try:
        mermaid_code = graph.get_graph().draw_mermaid()
        return mermaid_code
    except Exception as e:
        logger.warning("Failed to generate mermaid for graph '%s': %s", name, e)
        return f"%% Error generating mermaid for graph: {name}\n%% Error: {e}"


def export_all_graphs_to_mermaid() -> None:
    """将所有 agent graph 导出为 mermaid 文件"""
    try:
        # 延迟导入,避免循环依赖
        from app.graph.graphs import (
            get_incoming_message_graph,
            get_memory_update_graph,
            get_preview_graph,
            get_proactive_chat_graph,
        )

        # 定义要导出的 graph
        graphs = {
            "incoming_message_graph": get_incoming_message_graph(),
            "memory_update_graph": get_memory_update_graph(),
            "preview_graph": get_preview_graph(),
            "proactive_chat_graph": get_proactive_chat_graph(),
        }

        # 创建导出目录
        AGENT_NODE_GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

        # 导出每个 graph
        exported_count = 0
        for graph_name, graph in graphs.items():
            mermaid_code = _get_graph_mermaid(graph, graph_name)
            output_file = AGENT_NODE_GRAPHS_DIR / f"{graph_name}.md"
            
            # 添加标题和 mermaid 代码块
            content = f"# {graph_name}\n\n```mermaid\n{mermaid_code}\n```\n"
            output_file.write_text(content, encoding="utf-8")
            exported_count += 1
            logger.info("Exported graph '%s' to %s", graph_name, output_file)

        logger.info(
            "Successfully exported %d graph(s) to %s",
            exported_count,
            AGENT_NODE_GRAPHS_DIR,
        )

    except Exception as e:
        logger.exception("Failed to export agent graphs: %s", e)
