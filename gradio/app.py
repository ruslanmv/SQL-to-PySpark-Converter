import gradio

def hello(inp):
  return f"Hello {inp}!!"

# For information on Interfaces, head to https://gradio.app/docs/
# For user guides, head to https://gradio.app/guides/
# For Spaces usage, head to https://huggingface.co/docs/hub/spaces
iface = gradio.Interface(
  fn=hello,
  inputs='text',
  outputs='text',
  title='Hello World', 
  description='The simplest interface!')  

iface.launch()