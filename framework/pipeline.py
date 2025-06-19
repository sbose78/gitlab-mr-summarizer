from framework.interfaces import ContextGenerator, Analyzer, Notifier

class Pipeline:
    def __init__(self, context_generators, analyzers, notifiers):
        self.context_generators = context_generators
        self.analyzers = analyzers
        self.notifiers = notifiers

    def run(self, initial_context):
        context = dict(initial_context)
        # Generate context, respecting dependencies
        for gen in self.context_generators:
            missing = [req for req in gen.requires() if req not in context]
            if missing:
                raise Exception(f"Missing required context for {gen}: {missing}")
            gen.generate(context)
        # Run analyzers
        analysis_results = {}
        for analyzer in self.analyzers:
            key = getattr(analyzer, 'result_key', analyzer.__class__.__name__)
            analysis_results[key] = analyzer.analyze(context)
        # Notify
        for notifier in self.notifiers:
            notifier.notify(context, analysis_results)
        return analysis_results 