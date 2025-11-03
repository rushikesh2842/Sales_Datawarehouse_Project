from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as event_targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_glue as glue,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_iam as iam,
)
from constructs import Construct

class ETLPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        
        data_bucket = s3.Bucket(self, "DemoDataBucket",
            event_bridge_enabled=True
        )
        
        notification_topic = sns.Topic(self, "DemoNotificationTopic")
        notification_topic.add_subscription(sns_subs.EmailSubscription("rushi142019@gmail.com"))

        glue_role = iam.Role(self, "DemoGlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )

        glue_job = glue.CfnJob(self, "DemoGlueJob",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location="s3://aws-glue-assets-348962335134-us-east-1/scripts/hackathon_8_Glue_Script.py",
                python_version="3"
            )
        )
        
        redshift_db_user = "admin"
        glue_task = sfn_tasks.GlueStartJobRun(
            self, "GlueETLTask",
            glue_job_name=glue_job.ref,
            arguments=sfn.TaskInput.from_object({
                '--curated_path': "s3://DemoDataBucket/curated/maze/",
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.GlueOutput"
        )

        redshift_task = sfn_tasks.CallAwsService(
            self, "RedshiftSQLTask",
            service="redshiftdata",
            action="executeStatement",
            parameters={
                "ClusterIdentifier": "etl-pipeline-redshift-cluster",
                "Database": "dev",
                "DbUser": redshift_db_user,
                "Sql": "CALL blogdemo_proc.elt_rideshare(:bucketname,:manifestfile)",
                "Parameters": [
                    {"Name": "manifestfile", "Value.$": "$.GlueOutput.manifestpath"},
                    {"Name": "bucketname", "Value.$": "$.bucketname"}
                ]
            },
            iam_resources=["*"]
        )

        sns_task = sfn_tasks.SnsPublish(
            self, "NotifySnsTask",
            topic=notification_topic,
            message=sfn.TaskInput.from_text("ETL Workflow completed.")
        )
        
        definition = glue_task.next(redshift_task).next(sns_task)

        state_machine_role = iam.Role(self, "DemoSFNRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueConsoleFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSNSFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftDataFullAccess")

            ]
        )
        
        state_machine_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "redshift-data:ExecuteStatement",
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult",
                "redshift:GetClusterCredentials"
            ],
            resources=["*"]
        ))

        state_machine_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:ListBucket"
            ],
            resources=[
                "arn:aws:s3:::dehlive-sales-348962335134-us-east-1",
                "arn:aws:s3:::dehlive-sales-348962335134-us-east-1/*"
            ]
        ))

        state_machine = sfn.StateMachine(
            self, "DemoStateMachine",
            definition=definition,
            role=state_machine_role
        )
        
        event_rule = events.Rule(
            self, "DemoS3PutRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                resources=[data_bucket.bucket_arn]
            ),
            targets=[event_targets.SfnStateMachine(state_machine)]
        )
        
        data_bucket.grant_read_write(glue_role)
        data_bucket.grant_read(state_machine_role)
        notification_topic.grant_publish(state_machine_role)