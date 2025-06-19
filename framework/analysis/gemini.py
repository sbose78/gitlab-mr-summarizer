from typing import Dict, Any, Set
from vertexai.generative_models import GenerativeModel, GenerationConfig

class SimpleLLMAnalyzer:
    def __init__(self, model: GenerativeModel, config: GenerationConfig, prompt_template: str, input_keys: Set[str], result_key=None):
        self.model = model
        self.config = config
        self.prompt_template = prompt_template
        self.input_keys = input_keys
        self.result_key = result_key or self.__class__.__name__

    def analyze(self, context: Dict[str, Any]) -> str:
        prompt = self.prompt_template.format(**{k: context[k] for k in self.input_keys})
        print(f"Prompt: {len(prompt)}")
        response = self.model.generate_content(prompt, generation_config=self.config)
        #print(f"Response from llm: {len(response.text)}")
        return response.text 