"""
Literal 类型注解 Demo
=====================
运行：python literal_demo.py
类型检查（推荐）：pip install mypy && mypy literal_demo.py
"""

from typing import Literal


# ─────────────────────────────────────────────
# 1. 最基础用法：限定参数只能是几个固定字符串
# ─────────────────────────────────────────────
def set_log_level(level: Literal["DEBUG", "INFO", "WARN", "ERROR"]) -> None:
    print(f"日志等级设置为: {level}")


set_log_level("INFO")     # ✓ 合法
set_log_level("DEBUG")    # ✓ 合法
# set_log_level("TRACE")  # ❌ mypy 会报错：Argument has incompatible type
                          #    运行时 Python 不会报错，注解只是"提示"


# ─────────────────────────────────────────────
# 2. 限定数字、布尔等任意字面量
# ─────────────────────────────────────────────
def set_http_status(code: Literal[200, 404, 500]) -> str:
    return f"HTTP {code}"


print(set_http_status(200))
print(set_http_status(404))
# print(set_http_status(418))  # ❌ mypy 报错


def toggle(flag: Literal[True]) -> None:
    """这个函数只接受 True，不接受 False（罕见但合法）"""
    print(f"已开启，flag = {flag}")


toggle(True)
# toggle(False)  # ❌ mypy 报错


# ─────────────────────────────────────────────
# 3. 用作返回值类型（模拟 LangGraph 的路由器）
# ─────────────────────────────────────────────
END = "__end__"  # 模拟 LangGraph 的 END 常量


def should_continue(has_tool_call: bool) -> Literal["tool_node", "__end__"]:
    if has_tool_call:
        return "tool_node"
    return END  # 这里返回的是变量 END，但它的值是 "__end__"，符合 Literal


print("路由结果:", should_continue(True))
print("路由结果:", should_continue(False))


# ─────────────────────────────────────────────
# 4. 配合字典实现"穷举式分发"（很实用）
# ─────────────────────────────────────────────
Direction = Literal["up", "down", "left", "right"]


def move(direction: Direction) -> tuple[int, int]:
    deltas: dict[Direction, tuple[int, int]] = {
        "up": (0, 1),
        "down": (0, -1),
        "left": (-1, 0),
        "right": (1, 0),
    }
    return deltas[direction]


print("向上走:", move("up"))
print("向左走:", move("left"))
# print(move("forward"))  # ❌ mypy 报错


# ─────────────────────────────────────────────
# 5. Literal 不是运行时校验！这点必须记住
# ─────────────────────────────────────────────
def parse_mode(mode: Literal["read", "write"]) -> None:
    print(f"模式: {mode}")


# 下面这行运行时不会报错，Python 不会强制检查 Literal
# 只有 mypy / pyright 这类静态检查器才会拦住它
bad_value: str = "delete"
parse_mode(bad_value)  # type: ignore  # 运行时正常输出 "模式: delete"


# 想要运行时校验，要自己写：
def parse_mode_strict(mode: Literal["read", "write"]) -> None:
    if mode not in ("read", "write"):
        raise ValueError(f"非法的 mode: {mode!r}")
    print(f"模式（严格）: {mode}")


parse_mode_strict("read")
try:
    parse_mode_strict("delete")  # type: ignore
except ValueError as e:
    print(f"捕获异常: {e}")


# ─────────────────────────────────────────────
# 6. Literal 与 Union（| 运算符）的组合
# ─────────────────────────────────────────────
# Literal 本身可以多值列举，等价于 Union 多个 Literal
Status1 = Literal["pending", "done", "failed"]
Status2 = Literal["pending"] | Literal["done"] | Literal["failed"]  # 等价
# 类型检查器会把 Status1 和 Status2 视为同一个类型


def report(status: Status1) -> None:
    print(f"状态: {status}")


report("pending")
report("done")


# ─────────────────────────────────────────────
# 7. 实战小例子：模拟 LangGraph 的条件边路由器
# ─────────────────────────────────────────────
class FakeMessage:
    def __init__(self, tool_calls: list[str] | None = None):
        self.tool_calls = tool_calls or []


def langgraph_router(
    last_message: FakeMessage,
) -> Literal["tool_node", "__end__"]:
    if last_message.tool_calls:
        return "tool_node"
    return "__end__"


print("\n=== 模拟 LangGraph 路由 ===")
msg_with_tool = FakeMessage(tool_calls=["search"])
msg_without_tool = FakeMessage()

print("有 tool_calls →", langgraph_router(msg_with_tool))
print("无 tool_calls →", langgraph_router(msg_without_tool))


# ─────────────────────────────────────────────
# 8. 一个小技巧：用 get_args 拿到 Literal 的所有合法值
# ─────────────────────────────────────────────
from typing import get_args

print("\nDirection 的所有合法值:", get_args(Direction))
# 输出: ('up', 'down', 'left', 'right')

# 这在写"参数校验""自动生成 CLI 选项"时特别有用
allowed = get_args(Direction)
user_input = "up"
if user_input in allowed:
    print(f"{user_input} 是合法方向")
else:
    print(f"{user_input} 不合法，必须是 {allowed}")


print("\n✅ Demo 跑完了。建议安装 mypy 看类型检查效果：")
print("   pip install mypy")
print("   mypy literal_demo.py")