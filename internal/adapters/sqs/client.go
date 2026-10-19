package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

// ClientConfig descreve como chegar ao SQS. Endpoint vazio usa o da AWS; no
// ambiente local aponta para o MiniStack. As credenciais seguem a cadeia
// padrão da AWS (variáveis AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, perfil,
// papel de IAM...).
type ClientConfig struct {
	Region   string
	Endpoint string
}

func NewClient(ctx context.Context, cfg ClientConfig) (*awssqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS configuration: %w", err)
	}
	return awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}
