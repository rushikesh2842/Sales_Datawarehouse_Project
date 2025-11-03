import aws_cdk as cdk
from CDK_stack import ETLPipelineStack

app = cdk.App()
ETLPipelineStack(app, "ETLPipelineStack")
app.synth()