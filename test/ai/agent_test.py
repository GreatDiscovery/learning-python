import unittest


class TestAgent(unittest.TestCase):
    def test_deep_agent(self):
        # pip install -qU deepagents
        from deepagents import create_deep_agent

        def get_weather(city: str) -> str:
            """Get weather for a given city."""
            return f"It's always sunny in {city}!"

        agent = create_deep_agent(
            tools=[get_weather],
            system_prompt="You are a helpful assistant",
        )

        # Run the agent
        agent.invoke(
            {"messages": [{"role": "user", "content": "what is the weather in sf"}]}
        )

    def test_openai_agent(self):
        from langchain_openai import ChatOpenAI
        from langchain.tools import tool
        from langchain.agents import create_agent

        # 1️⃣ 定义工具
        @tool
        def add(a: int, b: int) -> int:
            """Add two numbers"""
            return a + b

        # 2️⃣ 模型
        # start llm server： vllm serve Qwen/Qwen2.5-0.5B-Instruct --enable-auto-tool-choice --tool-call-parser hermes
        model = ChatOpenAI(model="Qwen/Qwen2.5-0.5B-Instruct", base_url="http://localhost:8000/v1", temperature=0)

        # 3️⃣ 创建Agent（新版）
        agent = create_agent(
            model=model,
            tools=[add],
        )

        # 4️⃣ 调用
        result = agent.invoke(
            {"messages": [{"role": "user", "content": "3加5是多少"}]}
        )

        print(result["messages"][-1].content)
