from typing import TypedDict, Annotated
import operator
from langgraph.graph import StateGraph, START, END

# 使用pegel图执行算法实现的框架，被用在langgraph调度里
class State(TypedDict):
    messages: Annotated[list[str], operator.add]
    counter: int


def node_a(state: State) -> dict:
    print(f"[A] counter={state['counter']}")
    return {
        "messages": [f"A processed counter={state['counter']}"],
        "counter": state["counter"] + 1,
    }


def node_b(state: State) -> dict:
    print(f"[B] counter={state['counter']}")
    return {
        "messages": [f"B processed counter={state['counter']}"],
        "counter": state["counter"] + 10,
    }


def should_continue(state: State) -> str:
    if state["counter"] < 30:
        return "node_a"
    return END


if __name__ == '__main__':
    graph = StateGraph(State)
    graph.add_node("node_a", node_a)
    graph.add_node("node_b", node_b)
    graph.add_edge(START, "node_a")
    graph.add_edge("node_a", "node_b")
    graph.add_conditional_edges("node_b", should_continue)

    app = graph.compile()

    # 验证一下:它就是 Pregel
    from langgraph.pregel import Pregel
    print("type:", type(app), "is Pregel:", isinstance(app, Pregel))
    print()

    # 普通调用
    print("=== invoke ===")
    print(app.invoke({"messages": [], "counter": 0}))
    print()

    # 看每一步的 state 快照
    print("=== stream values ===")
    for snapshot in app.stream({"messages": [], "counter": 0}, stream_mode="values"):
        print(snapshot)
    print()

    # 看底层调度细节(这就是 PregelLoop 在每个 superstep 做的事)
    print("=== stream debug ===")
    for event in app.stream({"messages": [], "counter": 0}, stream_mode="debug"):
        print(event)

