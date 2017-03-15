# Automao.Data

建议数据库连接字符串的配置项存放在业务系统的主插件配置文件中，譬如插件目录结构中的 `Automao.SaaS.option` 或 `Automao.Common.option` 配置文件：

- [plugins]
	- [Automao]
		- **Automao.SaaS.option**

		- [Automao.Common]
			- Automao.Common.dll
			- Automao.Common.plugin
			- **Automao.Common.option**

			- Automao.Common.Web.dll
			- Automao.Common.Web.plugin
			- Automao.Common.Web.option

		- [Automao.Community]
		- [Automao.Customers]
		- [Automao.Marketing]
		- [Automao.Maintenances]

	- [Zongsoft.Security]
	- [Zongsoft.Externals]
		- [Json]
		- [Redis]
		- [Aliyun]


配置文件中数据连接字符串配置大致如下所示，指定的配置项路径为 `/Data/ConnectionStrings['Automao.SaaS']`。

```xml
<options>
	<option path="/Data">
		<connectionStrings>
			<connectionString name="Automao.SaaS"
			                  provider="MySql.Data.MySqlClient"
			                  value="server=127.0.0.1:3306;user id=root;Password=******;database=db;persist security info=False;Charset=utf8;Convert zero Datetime=true;Allow zero Datetime=True;" />
		</connectionStrings>
	</option>
</options>
```
