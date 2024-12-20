import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as s3d from "aws-cdk-lib/aws-s3-deployment";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as eventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as path from "path";

type S3EventNotificationDestinations =
  | s3n.LambdaDestination
  | s3n.SnsDestination
  | s3n.SqsDestination;

interface EventNotificationConfiguration {
  eventType: s3.EventType;
  getDestination: () => S3EventNotificationDestinations;
  filters: {
    prefix?: string;
    suffix?: string;
  };
}

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const app = new cdk.App();
    const stack = new cdk.Stack(app, "BatchEmailServiceMainStack");

    const awsRegion = stack.region;
    const accountId = stack.account;

    // SQS Queues:
    const failedEmailBatchQueue: sqs.Queue = new sqs.Queue(
      stack,
      "failedEmailBatchQueue",
      {
        queueName: "failedEmailBatchQueue",
        retentionPeriod: cdk.Duration.days(14),
      }
    );

    const emailBatchQueue: sqs.Queue = new sqs.Queue(stack, "emailBatchQueue", {
      queueName: "emailBatchQueue",
      deadLetterQueue: {
        queue: failedEmailBatchQueue,
        maxReceiveCount: 3,
      },
      visibilityTimeout: cdk.Duration.minutes(1),
    });

    // S3 Buckets:
    const s3BucketLifeCycleRule: s3.LifecycleRule = {
      abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
      expiredObjectDeleteMarker: true,
    };

    const jcBatchEmailServiceBucket: s3.Bucket = new s3.Bucket(
      stack,
      "jcBatchEmailServiceBucket",
      {
        lifecycleRules: [s3BucketLifeCycleRule],
        versioned: true,
        autoDeleteObjects: true,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }
    );

    // Deploy initial asset files to S3 bucket
    new s3d.BucketDeployment(stack, "DeployInitialAssets", {
      destinationBucket: jcBatchEmailServiceBucket,
      sources: [s3d.Source.asset(path.join(__dirname, "../assets"))],
      exclude: ["**/.DS_Store"],
    });

    // Lambdas:
    const sendBatchEmailEvent: lambda.Function = new lambda.Function(
      stack,
      "sendBatchEmailEvent",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "lambda_function.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(
            __dirname,
            "../lambdas/python/functions/send_batch_email_event"
          )
        ),
      }
    );

    const processBatchEmailEvent: lambda.Function = new lambda.Function(
      stack,
      "processBatchEmailEvent",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "lambda_function.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(
            __dirname,
            "../lambdas/python/functions/process_batch_email_event"
          )
        ),
      }
    );
    processBatchEmailEvent.addEventSource(
      new eventSources.SqsEventSource(emailBatchQueue, {
        batchSize: 1,
      })
    );

    const scheduleBatchEmail: lambda.Function = new lambda.Function(
      stack,
      "scheduleBatchEmail",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "lambda_function.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(
            __dirname,
            "../lambdas/python/functions/schedule_batch_email"
          )
        ),
      }
    );
    scheduleBatchEmail.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "scheduler:CreateSchedule",
          "scheduler:PutTargets",
          "scheduler:DeleteSchedule",
          "scheduler:TagResource",
        ],
        resources: [`arn:aws:scheduler:${awsRegion}:${accountId}:schedule/*`],
      })
    );

    const processSesTemplate: lambda.Function = new lambda.Function(
      stack,
      "processSesTemplate",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "lambda_function.lambda_handler",
        code: lambda.Code.fromAsset(
          path.join(
            __dirname,
            "../lambdas/python/functions/process_ses_template"
          )
        ),
      }
    );

    // Grant SQS Send and Consume Permissions:
    failedEmailBatchQueue.grantSendMessages(processBatchEmailEvent);
    emailBatchQueue.grantSendMessages(sendBatchEmailEvent);
    emailBatchQueue.grantConsumeMessages(processBatchEmailEvent);

    // S3 Event Notification Configuration
    const eventNotificationConfigurations: EventNotificationConfiguration[] = [
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(sendBatchEmailEvent),
        filters: {
          prefix: "batch/send/*",
          suffix: ".csv",
        },
      },
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(scheduleBatchEmail),
        filters: {
          prefix: "batch/scheduled/*",
          suffix: ".csv",
        },
      },
      {
        eventType: s3.EventType.OBJECT_CREATED,
        getDestination: () => new s3n.LambdaDestination(processSesTemplate),
        filters: {
          prefix: "templates/*",
          suffix: ".html",
        },
      },
      {
        eventType: s3.EventType.OBJECT_REMOVED,
        getDestination: () => new s3n.LambdaDestination(processSesTemplate),
        filters: {
          prefix: "templates/*",
          suffix: ".html",
        },
      },
    ];

    eventNotificationConfigurations.forEach((config) => {
      jcBatchEmailServiceBucket.addEventNotification(
        config.eventType,
        config.getDestination(),
        config.filters
      );
    });
  }
}
