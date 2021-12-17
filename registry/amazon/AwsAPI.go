package amazon

import (
	"JDSys/utils"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/elbv2"
)

var ELB = utils.ELB_ARN
var CRS = utils.AWS_CRED_PATH
var AUS = utils.AUTOSCALING_NAME

/*
Struttura contenente tutte le informazioni riguardanti un'istanza EC2
*/
type InstanceEC2 struct {
	ID, PrivateIP string
}

/*
Lista che mantiene tutte le attività di terminazione già processate
*/
var activity_cache []string

/*
Crea una sessione client AWS
*/
func CreateSession() *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials(CRS, "default")})
	if err != nil {
		utils.PrintTs(err.Error())
	}
	return sess
}

/*
Ottiene tutte le informazioni relative al Target Group specificato
*/
func getTargetGroup(elbArn string) *elbv2.DescribeTargetGroupsOutput {
	sess := CreateSession()
	svc := elbv2.New(sess)
	input := &elbv2.DescribeTargetGroupsInput{
		LoadBalancerArn: aws.String(elbArn),
	}
	result, err := svc.DescribeTargetGroups(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case elbv2.ErrCodeLoadBalancerNotFoundException:
				utils.PrintTs(elbv2.ErrCodeLoadBalancerNotFoundException + " " + aerr.Error())
			case elbv2.ErrCodeTargetGroupNotFoundException:
				utils.PrintTs(elbv2.ErrCodeTargetGroupNotFoundException + " " + aerr.Error())
			default:
				utils.PrintTs(aerr.Error())
			}
		} else {
			utils.PrintTs(err.Error())
		}
	}
	return result
}

/*
Ottiene lo stato delle istanze collegate al Target Group specificato
*/
func getTargetsHealth(targetGroupArn string) *elbv2.DescribeTargetHealthOutput {
	sess := CreateSession()
	svc := elbv2.New(sess)
	input := &elbv2.DescribeTargetHealthInput{
		TargetGroupArn: aws.String(targetGroupArn),
	}

	result, err := svc.DescribeTargetHealth(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case elbv2.ErrCodeInvalidTargetException:
				utils.PrintTs(elbv2.ErrCodeInvalidTargetException + " " + aerr.Error())
			case elbv2.ErrCodeTargetGroupNotFoundException:
				utils.PrintTs(elbv2.ErrCodeTargetGroupNotFoundException + " " + aerr.Error())
			case elbv2.ErrCodeHealthUnavailableException:
				utils.PrintTs(elbv2.ErrCodeHealthUnavailableException + " " + aerr.Error())
			default:
				utils.PrintTs(aerr.Error())
			}
		} else {
			utils.PrintTs(err.Error())
		}
	}
	return result
}

/*
Ottiene gli ID associati a tutte le istanze healthy del target group
*/
func getHealthyInstancesId(targetHealth *elbv2.DescribeTargetHealthOutput) []string {
	var healthyNodes []string
	descriptions := targetHealth.TargetHealthDescriptions
	for i := 0; i < len(descriptions); i++ {
		actual := descriptions[i].String()
		id := utils.GetStringInBetween(actual, "Id: \"", "\",")
		state := utils.GetStringInBetween(actual, "State: \"", "\"")
		if state == "healthy" {
			healthyNodes = append(healthyNodes, id)
		}
	}
	return healthyNodes
}

/*
Ottiene tutte le informazioni di una istanza EC2 tramite il suo ID
*/
func getInstanceInfo(instanceId string) *ec2.DescribeInstancesOutput {
	sess := CreateSession()
	svc := ec2.New(sess)
	input := &ec2.DescribeInstancesInput{
		InstanceIds: []*string{
			aws.String(instanceId),
		},
	}

	result, err := svc.DescribeInstances(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				utils.PrintTs(aerr.Error())
			}
		} else {
			utils.PrintTs(err.Error())
		}
	}
	return result
}

/*
Ottiene ID, Indirizzo Pubblico e Indirizzo Privato di una istanza EC2
*/
func getInstance(instanceInfo *ec2.DescribeInstancesOutput) InstanceEC2 {
	descriptions := instanceInfo.Reservations
	actual := descriptions[0].String()
	id := utils.GetStringInBetween(actual, "InstanceId: \"", "\",")
	private := utils.GetStringInBetween(actual, "PrivateIpAddress: \"", "\"")
	return InstanceEC2{id, private}
}

/*
Ritorna gli indirizzi IP di tutti i nodi connessi al load balancer
*/
func GetActiveNodes() []InstanceEC2 {
	var nodes []InstanceEC2
	targetGroup := getTargetGroup(ELB)
	targetGroupArn := utils.GetStringInBetween(targetGroup.String(), "TargetGroupArn: \"", "\",")
	targetsHealth := getTargetsHealth(targetGroupArn)
	healthyInstancesList := getHealthyInstancesId(targetsHealth)

	nodes = make([]InstanceEC2, len(healthyInstancesList))
	for i := 0; i < len(healthyInstancesList); i++ {
		instance := getInstanceInfo(healthyInstancesList[i])
		nodes[i] = getInstance(instance)
	}
	return nodes
}

/*
Ottiene dal Load Balancer la lista delle attività schedulate relative a ScaleIN e ScaleOUT.
*/
func getScalingActivities() *autoscaling.DescribeScalingActivitiesOutput {
	sess := CreateSession()
	svc := autoscaling.New(sess)
	input := &autoscaling.DescribeScalingActivitiesInput{
		AutoScalingGroupName: aws.String(AUS),
	}

	result, err := svc.DescribeScalingActivities(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case autoscaling.ErrCodeInvalidNextToken:
				utils.PrintTs(autoscaling.ErrCodeInvalidNextToken + " " + aerr.Error())
			case autoscaling.ErrCodeResourceContentionFault:
				utils.PrintTs(autoscaling.ErrCodeResourceContentionFault + " " + aerr.Error())
			default:
				utils.PrintTs(aerr.Error())
			}
		} else {
			utils.PrintTs(err.Error())
		}
	}
	return result
}

/*
Ottiene gli ID di tutte le istanze che sono nello stato di terminazione
*/
func GetTerminatingInstances() []InstanceEC2 {

	activityList := getScalingActivities()

	var terminatingNodes []InstanceEC2
	activities := activityList.Activities
	TERMINATING_START := "Description: \"Terminating EC2 instance: "
	TERMINATING_END := " -"

	for i := 0; i < len(activities); i++ {
		actual := activities[i].String()
		progress := utils.GetStringInBetween(actual, "Progress: ", ",")
		if progress != "100" {
			status := utils.GetStringInBetween(actual, "StatusCode: \"", "\"\n")
			if status == "WaitingForELBConnectionDraining" {
				nodeId := utils.GetStringInBetween(actual, TERMINATING_START, TERMINATING_END)
				if utils.StringInSlice(nodeId, activity_cache) {
					continue
				}
				utils.PrintHeaderL3(nodeId + " is terminating")
				instanceInfo := getInstanceInfo(nodeId)
				instance := getInstance(instanceInfo)
				terminatingNodes = append(terminatingNodes, instance)
				activity_cache = append(activity_cache, nodeId)
			}
		}
	}
	return terminatingNodes
}

/*
Pulisce periodicamente la cache sulle istanze in terminazione
*/
func Start_cache_flush_service() {
	for {
		time.Sleep(utils.ACTIVITY_CACHE_FLUSH_INTERVAL)
		activity_cache = nil
	}
}
