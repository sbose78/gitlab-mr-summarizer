from abc import ABC, abstractmethod

class ContextGenerator(ABC):
    @abstractmethod
    def requires(self):
        """Return a set of context keys this generator depends on."""
        pass

    @abstractmethod
    def provides(self):
        """Return a set of context keys this generator provides."""
        pass

    @abstractmethod
    def generate(self, context):
        """Generate context and update the context dict."""
        pass

class Analyzer(ABC):
    @abstractmethod
    def analyze(self, context):
        """Analyze using the context and return results."""
        pass

class Notifier(ABC):
    @abstractmethod
    def notify(self, context, analysis_results):
        """Send notifications based on context and analysis results."""
        pass 