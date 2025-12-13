### 1. Run dbt Transformations

After data is loaded into BigQuery, navigate to the dbt project and run transformations:

```bash
cd brazillian_ecommerce_project
```

Run **dbt clean** to clean any existing dependencies

```bash
dbt clean
```
Run **dbt deps** to install packages from packages.yml

```bash
dbt deps
```
Run **dbt run** to materializes dbt models (tables, views) 

```bash
dbt run
```
Run **dbt test** to executes the data quality tests defined in the dbt project

```bash
dbt test
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
