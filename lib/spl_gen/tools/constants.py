from .write_spl import write_spl_tool_func
from .run_inference import run_inference_func

TOOL_MAPPING = {
    "write_spl": write_spl_tool_func,
    "inference": run_inference_func,
}