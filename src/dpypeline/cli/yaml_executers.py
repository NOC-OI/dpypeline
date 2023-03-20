"""pyyaml exectures."""
import importlib


def function_executer(loader, node):
    """Execute a function."""
    params = loader.construct_mapping(node)
    module_name, func_name = params["function"].rsplit(".", 1)
    module = importlib.import_module(module_name)
    func = getattr(module, func_name)
    kwargs = {key: params[key] for key in params if key != "function"}
    return func(**kwargs)


def method_executer(loader, node):
    """Execute a method."""
    params = loader.construct_mapping(node)
    instance = params["instance"]
    method = getattr(instance, params["method"])
    kwargs = {key: params[key] for key in params if key not in ["instance", "method"]}
    return method(**kwargs)


returners_dict = {
    "!Function": function_executer,
    "!Method": method_executer,
}
