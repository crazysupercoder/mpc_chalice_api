# MPC
MPC Chalice API

### System requirements
- AWS CLI
- Python 3.6+
- Pip 18.0+(If pip version is larger than 19.2, chalice will not be able to be installed)

### Setup
- Install virtualenv (Windows)
```
virtualenv venv
cd venv/Scripts
activate
python -m pip install pip==18.0
```
- Install virtualenv (Linux)
```
virtualenv venv
source venv/bin/activate
python -m pip install pip==18.0

```
- Install packages  `pip install -r requirements.txt`


### AWS Config
```
[mpc]
aws_access_key_id = Your Access Key
aws_secret_access_key = Your Secret Key
region = Team Region(eu-west-1)
```
### Database
- AWS DynamoDB 
- Add sample data
For example, the sample data of `Banners` table
`aws dynamodb batch-write-item --request-items file://Banners.json --profile=mpc`

For product, elasticsearch is used.
- To use sort of any field, `fielddata` of the field should be `true`
```
https://1vobn9mvgc.execute-api.eu-west-1.amazonaws.com/api/products/set-fielddata-true
{
	"field": "product_name",
	"type": "text"
}
```

### Deployment
Before deployment you should specify suffix of the app you are deploying to, as following.
When you are deploying to dev server, please specify your name as suffix.
```
$ export APP_NAME_SUFFIX=liming
```
Or if it should be deployed to a specific environment like staging or production, you can specify the suffix with the stage itself.
```
$ export APP_NAME_SUFFIX=staging
```


```
$ chalice deploy --stage=liming --profile=mpc
```