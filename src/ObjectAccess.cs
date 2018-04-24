/*
 * Authors:
 *   喻星(Xing Yu) <491718907@qq.com>
 *   钟峰(Popeye Zhong) <9555843@qq.com>
 *
 * Copyright (C) 2015-2017 Automao Network Co., Ltd. <http://www.zongsoft.com>
 *
 * This file is part of Automao.Data.
 *
 * Automao.Data is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * Automao.Data is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Automao.Data; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

using System;
using System.ComponentModel;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Automao.Data.Mapping;

using Zongsoft.Data;
using Zongsoft.Common;

namespace Automao.Data
{
	public abstract class ObjectAccess : DataAccessBase
	{
		#region 私有字段
		private ConcurrentDictionary<Type, EntityDesciptior> _entityCache;
		#endregion

		#region 字段
		private SqlExecuter _executer;
		private DbProviderFactory _providerFactory;
		private MappingInfo _mappingInfo;
		private ConcurrentDictionary<Type, KeyValuePair<System.Reflection.ParameterInfo[], System.Reflection.PropertyInfo[]>> _typeDictionary;
		#endregion

		#region 构造函数
		protected ObjectAccess(DbProviderFactory providerFactory)
		{
			_entityCache = new ConcurrentDictionary<Type, EntityDesciptior>();
			_providerFactory = providerFactory;
			_typeDictionary = new ConcurrentDictionary<Type, KeyValuePair<System.Reflection.ParameterInfo[], System.Reflection.PropertyInfo[]>>();
		}
		#endregion

		#region 属性
		public string ConnectionPath
		{
			get;
			set;
		}

		protected MappingInfo MappingInfo
		{
			get
			{
				if(_mappingInfo == null)
				{
					var temp = new MappingInfo();
					System.Threading.Interlocked.CompareExchange(ref _mappingInfo, temp, null);

					if(_mappingInfo == temp)
						_mappingInfo = MappingInfo.Create();
				}

				return _mappingInfo;
			}
		}

		internal SqlExecuter Executer
		{
			[System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
			get
			{
				if(_executer == null)
				{
					_executer = SqlExecuter.GetInstance(_providerFactory);

					if(string.IsNullOrWhiteSpace(this.ConnectionPath))
						throw new InvalidOperationException("Missing option path of connection string.");

					//获取连接字符串配置项
					var config = Zongsoft.ComponentModel.ApplicationContextBase.Current.OptionManager.GetOptionValue(this.ConnectionPath);

					if(config == null)
						throw new InvalidOperationException($"Not found connection string by '{this.ConnectionPath}' option path.");

					if(config is string)
						_executer.ConnectionString = (string)config;
					else if(config is Zongsoft.Options.Configuration.ConnectionStringElement)
						_executer.ConnectionString = ((Zongsoft.Options.Configuration.ConnectionStringElement)config).Value;
				}

				return _executer;
			}
		}
		#endregion

		#region 虚拟属性
		protected virtual bool IsScalarType(Type type)
		{
			return Zongsoft.Common.TypeExtension.IsScalarType(type) || typeof(Expression).IsAssignableFrom(type);
		}
		#endregion

		#region Base成员
		public override string[] GetKey(string name)
		{
			if(string.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			var info = MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(info == null)
				throw new Exception($"未找到 '{name}' 对应的数据映射。");

			return info.PropertyNodeList.Where(p => p.IsKey).Select(p => p.Name).ToArray();
		}

		#region 查询
		protected override void OnSelect<T>(DataSelectionContext context)
		{
			var name = context.Name;
			var condition = context.Condition;
			var grouping = context.Grouping;
			var scope = context.Scope;
			var paging = context.Paging;
			var sortings = context.Sortings;

			var members = this.ResolveScope(name, scope, context.EntityType);

			if(string.IsNullOrEmpty(name))
				name = context.EntityType.Name;

			var classNode = MappingInfo.ClassNodeList?.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(classNode == null)
				throw new DataException(string.Format("未找到{0}对应的mapping节点", name));

			var classInfo = CreateClassInfo("T", classNode);

			var allColumnInfos = new Dictionary<string, ColumnInfo>();

			var conditionNames = GetConditionName(condition) ?? new string[0];
			var allColumns = new List<string>(conditionNames);

			if(grouping != null)
			{
				allColumns.AddRange(grouping.Keys.Select(p => p.Name));

				if(grouping.Filter != null)
					allColumns.AddRange(GetConditionName(grouping.Filter));

				if(!members.Any(p => p.StartsWith("count(", StringComparison.OrdinalIgnoreCase)))
					members = members.Concat(new[] { "COUNT(0)" }).ToArray();
			}

			if(sortings != null && sortings.Length > 0)
			{
				foreach(var item in sortings)
				{
					allColumns.Add(item.Name);
				}
			}

			allColumns.AddRange(members);
			allColumnInfos = CreateColumnInfo(allColumns, classInfo);

			context.Result = new ObjectAccessResult<T>(p =>
			{
				classInfo.SetIndex(p.TableIndex++);
				p.JoinStartIndex = classInfo.SetJoinIndex(p.JoinStartIndex);

				var parameter = new CreatingSelectSqlParameter(p);
				parameter.ClassInfo = classInfo;
				parameter.Condition = condition;
				parameter.Members = members;
				parameter.ConditionNames = conditionNames;
				parameter.AllColumnInfos = allColumnInfos;
				parameter.Paging = paging;
				parameter.Grouping = grouping;
				parameter.Sortings = sortings;
				parameter.ConditionOperator = p.ConditionOperator;
				return CreateSelectSql(parameter);
			}, p =>
			{
				IEnumerable<Dictionary<string, object>> tablevalues;

				try
				{
					tablevalues = this.Executer.Select(p.Sql, this.CreateParameters(0, p.Values));
				}
				catch(global::MySql.Data.MySqlClient.MySqlException ex)
				{
					throw ExceptionUtility.GetDataException(ex);
				}

				var result = this.SetEntityValue<T>(tablevalues, classInfo);
				return result;
			});
		}

		internal CreateSqlResult CreateSelectSql(CreatingSelectSqlParameter parameter)
		{
			object[] values;

			Func<ColumnInfo, bool> predicate = (p =>
			{
				var columnInfo = p;
				if(columnInfo.PropertyNode != null)
					return !columnInfo.PropertyNode.Ignored;//排除列名为空的字段
				return !columnInfo.ClassInfo.ClassNode.JoinList.Any(pp => pp.Name == columnInfo.Field);//排除导航属性
			});

			#region 要查询的列
			var selectMembers = parameter.Members.Where(p => parameter.Grouping == null || p.Contains('(') && !parameter.Grouping.Keys.Any(m => string.Equals(m.Name, p, StringComparison.OrdinalIgnoreCase)));
			if(parameter.Grouping != null)
				selectMembers = selectMembers.Concat(parameter.Grouping.Keys.Select(p => p.Name));

			var columns = selectMembers.Select(p => parameter.AllColumnInfos[p]).Where(predicate).ToList();
			#endregion

			#region where
			int ti = parameter.TableIndex, ji = parameter.JoinStartIndex, vi = parameter.ValueIndex;
			var where = parameter.Condition.ToWhere(parameter.AllColumnInfos, ref ti, ref ji, ref vi, out values);
			parameter.TableIndex = ti;
			parameter.JoinStartIndex = ji;
			parameter.ValueIndex = vi;
			#endregion

			#region join
			var tempJoinInfos = new List<Join>();
			var tempColumns = parameter.ConditionNames.Concat(selectMembers);
			if(parameter.Grouping != null && parameter.Grouping.Filter != null)
				tempColumns = tempColumns.Concat(GetConditionName(parameter.Grouping.Filter));
			foreach(var item in tempColumns.ToArray())
			{
				var columnInfo = parameter.AllColumnInfos[item];
				if(columnInfo.Join != null && !tempJoinInfos.Contains(columnInfo.Join))
				{
					tempJoinInfos.Add(columnInfo.Join);
					tempJoinInfos.AddRange(columnInfo.Join.GetParent(p => tempJoinInfos.Contains(p)));
				}
			}

			Pretreatment(tempJoinInfos);

			var join = string.Join(" ", tempJoinInfos.OrderBy(p => p.Target.AsIndex).Select(p => p.ToJoinSql(CreateColumnInfo)));
			#endregion

			#region grouping
			string having = string.Empty;
			string group = string.Empty;
			List<ColumnInfo> groupedSelectColumns = null;
			string groupedJoin = string.Empty;
			var newHostAsName = parameter.ClassInfo.As + (parameter.ClassInfo.AsIndex + 1);
			if(parameter.Grouping != null)
			{
				var groupColumnInfos = parameter.Grouping.Keys.Select(p => parameter.AllColumnInfos[p.Name]).ToArray();
				group = string.Format("GROUP BY {0}", string.Join(",", groupColumnInfos.Select(p => p.ToColumn())));

				var groupedSelectMembers = parameter.Members.Where(p => !p.Contains('(') && !parameter.Grouping.Keys.Any(m => string.Equals(m.Name, p, StringComparison.OrdinalIgnoreCase)));
				groupedSelectColumns = groupedSelectMembers.Select(p => parameter.AllColumnInfos[p]).Where(predicate).ToList();

				tempJoinInfos = new List<Join>();
				foreach(var item in groupedSelectMembers)
				{
					var columnInfo = parameter.AllColumnInfos[item];
					if(columnInfo.Join != null && !tempJoinInfos.Contains(columnInfo.Join))
					{
						tempJoinInfos.Add(columnInfo.Join);
						tempJoinInfos.AddRange(columnInfo.Join.GetParent(p => groupColumnInfos.Any(pp => pp.Join == p) || tempJoinInfos.Contains(p)));
					}
				}

				foreach(var item in tempJoinInfos)
				{
					if(groupColumnInfos.Any(gc => gc.Join == item.Parent))
					{
						if(item.JoinInfo.Type == JoinType.Left)
							item.ChangeMode(JoinType.Inner);
						item.ChangeHostAsName(newHostAsName);
					}
				}

				Pretreatment(tempJoinInfos);

				groupedJoin = string.Join(" ", tempJoinInfos.OrderBy(p => p.Target.AsIndex).Select(p =>
				{
					var groupColumnInfo = groupColumnInfos.Where(gc => gc.Join == p.Parent);
					if(groupColumnInfo.Any())
					{
						var temp = groupColumnInfo.ToDictionary(gc => gc.Field, gc => gc);
						var dic = p.JoinInfo.Member.ToDictionary(c => temp[c.Key.Name].GetColumnEx(), c => c.Key.Field);

						return Join.CreatJoinSql(p, dic, CreateColumnInfo);
					}
					return p.ToJoinSql(CreateColumnInfo);
				}));

				if(parameter.Grouping.Filter != null)
				{
					object[] tempValues;
					ti = parameter.TableIndex;
					ji = parameter.JoinStartIndex;
					vi = parameter.ValueIndex;
					having = parameter.Grouping.Filter.ToWhere(parameter.AllColumnInfos, ref ti, ref ji, ref vi, out tempValues, "HAVING {0}");
					values = values.Concat(tempValues).ToArray();
				}
			}
			#endregion

			var orderby = string.Empty;

			if(parameter.Sortings != null && parameter.Sortings.Length > 0)
			{
				orderby = "ORDER BY {0}" + string.Join(",", parameter.Sortings.Select(p => p.Parse(parameter.AllColumnInfos)));
			}

			if(parameter.Paging != null && parameter.Paging.TotalCount == 0)
			{
				var countSql = string.Empty;
				if(string.IsNullOrEmpty(group))
				{
					var subparameter = new CreateSelectSqlParameter(false);
					subparameter.Info = parameter.ClassInfo;
					subparameter.Columns = new List<ColumnInfo>();
					subparameter.Columns.Add(CreateColumnInfo("COUNT(0)"));
					subparameter.Where = where;
					subparameter.Join = join;

					countSql = CreateSelectSql(subparameter);
				}
				else
				{
					var subparameter = new CreateGroupSelectSqlParameter(false);
					subparameter.Info = parameter.ClassInfo;
					subparameter.NewTableNameEx = newHostAsName;
					subparameter.Columns = columns;
					subparameter.Where = where;
					subparameter.Join = join;
					subparameter.Group = group;
					subparameter.Having = having;
					subparameter.GroupedSelectColumns = new List<ColumnInfo>();
					subparameter.GroupedSelectColumns.Add(CreateColumnInfo("COUNT(0)"));
					subparameter.GroupedJoin = groupedJoin;

					countSql = CreateSelectSql(subparameter);
				}

				IEnumerable<Dictionary<string, object>> tablevalues;

				try
				{
					tablevalues = this.Executer.Select(countSql, CreateParameters(0, values));
				}
				catch(global::MySql.Data.MySqlClient.MySqlException ex)
				{
					throw ExceptionUtility.GetDataException(ex);
				}

				if(tablevalues != null)
				{
					var item = tablevalues.FirstOrDefault();
					parameter.Paging.TotalCount = Zongsoft.Common.Convert.ConvertValue<int>(item.Values.First());
				}
			}

			string sql;
			if(string.IsNullOrEmpty(group))
			{
				var subparameter = new CreateSelectSqlParameter(parameter.Subquery);
				subparameter.Info = parameter.ClassInfo;
				subparameter.Columns = columns;
				subparameter.Where = where;
				subparameter.Join = join;
				subparameter.Orderby = orderby;
				subparameter.Paging = parameter.Paging;
				subparameter.ConditionOperator = parameter.ConditionOperator;

				sql = CreateSelectSql(subparameter);
			}
			else
			{
				var subparameter = new CreateGroupSelectSqlParameter(parameter.Subquery);
				subparameter.Info = parameter.ClassInfo;
				subparameter.NewTableNameEx = newHostAsName;
				subparameter.Columns = columns;
				subparameter.Where = where;
				subparameter.Join = join;
				subparameter.Group = group;
				subparameter.Having = having;
				subparameter.GroupedSelectColumns = groupedSelectColumns;
				subparameter.GroupedJoin = groupedJoin;
				subparameter.Orderby = orderby;
				subparameter.Paging = parameter.Paging;
				subparameter.ConditionOperator = parameter.ConditionOperator;

				sql = CreateSelectSql(subparameter);
			}

			//if(parameter.Paging != null && parameter.Paging.PageIndex > parameter.Paging.PageCount)
			//	parameter.Paging.PageIndex = parameter.Paging.PageCount;

			return new CreateSqlResult(sql, values);
		}

		protected abstract string CreateSelectSql(CreateSelectSqlParameter parameter);

		protected abstract string CreateSelectSql(CreateGroupSelectSqlParameter parameter);

		protected abstract ColumnInfo CreateColumnInfo(string original);

		protected abstract Dictionary<string, ColumnInfo> CreateColumnInfo(IEnumerable<string> columns, ClassInfo root);

		protected abstract ClassInfo CreateClassInfo(string @as, ClassNode classNode);

		protected abstract string GetProcedureName(ProcedureNode procedureNode);
		#endregion

		#region Count
		protected override void OnCount(DataCountContext context)
		{
			var name = context.Name;
			var condition = context.Condition;
			string[] includes = new string[0];

			if(context.Includes != null && context.Includes.Length > 0)
				includes = context.Includes.Split(',', ';');

			var info = this.MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var joinInfos = new Dictionary<string, Join>();
			var allColumnInfos = new Dictionary<string, ColumnInfo>();

			var conditionNames = GetConditionName(condition);
			var allcolumns = conditionNames.Concat(includes);

			var classInfo = CreateClassInfo("T", info);
			allColumnInfos = CreateColumnInfo(allcolumns, classInfo);
			classInfo.SetJoinIndex(0);

			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			object[] values;
			var whereSql = condition.ToWhere(allColumnInfos, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);

			var tempJoinInfos = new List<Join>();
			foreach(var item in allColumnInfos.Keys)
			{
				var columnInfo = allColumnInfos[item];
				if(columnInfo.Join != null && !tempJoinInfos.Contains(columnInfo.Join))
				{
					tempJoinInfos.Add(columnInfo.Join);
					tempJoinInfos.AddRange(columnInfo.Join.GetParent(p => tempJoinInfos.Contains(p)));
				}
			}
			var joinsql = string.Join(" ", tempJoinInfos.OrderBy(p => p.Target.AsIndex).Select(p => p.ToJoinSql(CreateColumnInfo)));

			string countSql;
			if(includes.Length == 0)
				countSql = "COUNT(0)";
			else if(includes.Length == 1)
				countSql = string.Format("COUNT({0})", allColumnInfos[includes[0]].ToColumn());
			else
				countSql = string.Format("COUNT(CONCAT({0}))", string.Join(",", includes.Select(p => allColumnInfos[p]).Select(p => p.ToColumn())));

			var sql = string.Format("SELECT {0} FROM {1} {2} {3}", countSql, classInfo.GetTableName(), joinsql, whereSql);

			var result = Executer.ExecuteScalar(sql, CreateParameters(0, values));
			context.Result = int.Parse(result.ToString());
		}
		#endregion

		#region 删除
		protected override void OnDelete(DataDeletionContext context)
		{
			var name = context.Name;
			var condition = context.Condition;
			var cascades = context.Cascades;

			if(cascades != null && cascades.Length > 0)
				throw new NotSupportedException();

			if(string.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			var info = this.MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var conditionNames = GetConditionName(condition);
			var classInfo = CreateClassInfo("", info);
			var columnInfos = CreateColumnInfo(conditionNames, classInfo);
			classInfo.SetJoinIndex(0);

			var values = new object[0];
			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			var whereSql = condition.ToWhere(columnInfos, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);

			var sql = string.Format("DELETE FROM {0} {1}", CreateClassInfo("", info).GetTableName(), whereSql);
			context.Count = Executer.Execute(sql, CreateParameters(0, values));
		}
		#endregion

		#region 执行
		protected override void OnExecute<T>(DataExecutionContext context)
		{
			var name = context.Name;
			var inParameters = context.InParameters;

			ClassNode classInfo = null;
			var procedureInfo = this.MappingInfo.ProcedureNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(procedureInfo == null)
			{
				classInfo = this.MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
				if(classInfo != null)
					procedureInfo = this.MappingInfo.ProcedureNodeList.FirstOrDefault(p => p.Name.Equals(classInfo.Table, StringComparison.OrdinalIgnoreCase));
			}

			if(procedureInfo == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			Dictionary<string, object> dic;
			var paramers = inParameters.Select((entry, i) =>
			{
				var parameterName = entry.Key;
				var dbType = "";
				bool isInOutPut = false;
				int? size = null;
				if(procedureInfo != null)
				{
					var item = procedureInfo.ParameterList.FirstOrDefault(pp => pp.Name.Equals(entry.Key, StringComparison.CurrentCultureIgnoreCase));
					if(item != null)
					{
						parameterName = item.Name;
						dbType = item.DbType;
						isInOutPut = item.IsOutPut;
						size = item.Size;
					}
				}
				return CreateParameter(i, entry.Value ?? DBNull.Value, dbType, parameterName, false, isInOutPut, size, true);
			}).ToList();

			paramers.AddRange(procedureInfo.ParameterList.Where(p => !inParameters.ContainsKey(p.Name)).Select((p, i) => CreateParameter(paramers.Count + i, null, p.DbType, p.Name, p.IsOutPut, false, p.Size, true)).ToArray());

			var procedureName = GetProcedureName(procedureInfo);
			var tablevalues = Executer.ExecuteProcedure(procedureName, paramers.ToArray(), out dic);

			context.OutParameters = dic.ToDictionary(p => p.Key, p => p.Value);
			context.Result = tablevalues.Select(p => (T)CreateEntity(typeof(T), p, null));
		}

		protected override void OnExecuteScalar(DataExecutionContext context)
		{
			var name = context.Name;
			var inParameters = context.InParameters;

			ClassNode classInfo = null;
			var procedureInfo = this.MappingInfo.ProcedureNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(procedureInfo == null)
			{
				classInfo = this.MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
				if(classInfo != null)
					procedureInfo = this.MappingInfo.ProcedureNodeList.FirstOrDefault(p => p.Name.Equals(classInfo.Table, StringComparison.OrdinalIgnoreCase));
			}

			if(procedureInfo == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			Dictionary<string, object> dic;
			var paramers = inParameters.Where(p => p.Value != null).Select((p, i) =>
			{
				var parameterName = p.Key;
				var dbType = "";
				bool isInOutPut = false;
				int? size = null;
				if(procedureInfo != null)
				{
					var item = procedureInfo.ParameterList.FirstOrDefault(pp => pp.Name.Equals(p.Key, StringComparison.CurrentCultureIgnoreCase));
					if(item != null)
					{
						parameterName = item.Name;
						dbType = item.DbType;
						isInOutPut = item.IsOutPut;
						size = item.Size;
					}
				}
				return CreateParameter(i, p.Value, dbType, parameterName, false, isInOutPut, size, true);
			}).ToList();

			paramers.AddRange(procedureInfo.ParameterList.Where(p => !inParameters.ContainsKey(p.Name)).Select((p, i) => CreateParameter(paramers.Count + i, null, p.DbType, p.Name, p.IsOutPut, false, p.Size, true)).ToArray());

			var procedureName = GetProcedureName(procedureInfo);
			var tablevalues = Executer.ExecuteProcedure(procedureName, paramers.ToArray(), out dic);

			context.OutParameters = dic.ToDictionary(p => p.Key, p => p.Value);

			if(classInfo != null)
				context.Result = tablevalues.Select(p => CreateEntity(classInfo.EntityType, p, classInfo));
			else
			{
				var item = tablevalues.FirstOrDefault();

				if(item == null || item.Count == 0)
					context.Result = item;
				else
					context.Result = item[item.Keys.FirstOrDefault()];
			}
		}
		#endregion

		#region 新增
		protected override void OnInsert(DataInsertionContext context)
		{
			var name = context.Name;
			var scope = context.Scope;

			IEnumerable<DataDictionary> items;

			if(context.IsMultiple)
				items = (IEnumerable<DataDictionary>)context.Data;
			else
				items = new DataDictionary[] { (DataDictionary)context.Data };

			var info = MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var insertCount = 0;
			var sqls = new Dictionary<DataDictionary, KeyValuePair<string, DbParameter[]>>();

			var insertformat = "INSERT INTO {0}({1}) VALUES({2})";
			var columnformat = "{0}";
			var tableName = CreateClassInfo("", info).GetTableName();

			foreach(var item in items)
			{
				if(item == null)
					continue;

				string[] includes = this.ResolveScope(name, scope, item);

				Dictionary<PropertyNode, object> pks;
				var dic = GetColumnFromEntity(info, item, null, out pks).Where(p => p.Value != null && includes.Contains(p.Key.Name, StringComparer.OrdinalIgnoreCase)).ToDictionary(p => p.Key, p => p.Value);

				var sql = string.Format(insertformat, tableName,
					string.Join(",", dic.Keys.Select(p => string.Format(columnformat, CreateColumnInfo(p.Field).ToColumn()))),
					string.Join(",", dic.Select((p, i) => string.Format("{{{0}}}", i)))
				);

				var paramers = dic.Select((p, i) => CreateParameter(i, p.Value)).ToArray();

				sqls.Add(item, new KeyValuePair<string, DbParameter[]>(sql, paramers));
			}

			using(var transaction = new Zongsoft.Transactions.Transaction())
			{
				foreach(var key in sqls.Keys)
				{
					var item = sqls[key];
					var count = this.Executer.Execute(item.Key, item.Value);

					if(count > 0)
					{
						insertCount++;

						if(info.PropertyNodeList.Any(p => p.Sequenced))
						{
							key.TrySet(info.PropertyNodeList.FirstOrDefault(p => p.Sequenced).Name, () => this.Executer.ExecuteScalar("SELECT LAST_INSERT_ID()", null));

							//var property = info.EntityType.GetProperty(info.PropertyNodeList.FirstOrDefault(p => p.Sequenced).Name);
							//if(property != null)
							//{
							//	var id = executer.ExecuteScalar("SELECT LAST_INSERT_ID()", null);
							//	if(id != null)
							//		property.SetValue(key, Zongsoft.Common.Convert.ConvertValue(id, property.PropertyType));
							//}
						}
					}
				}

				transaction.Commit();
			}

			context.Count = insertCount;
		}
		#endregion

		#region 修改
		protected override void OnUpdate(DataUpdationContext context)
		{
			var name = context.Name;
			var condition = context.Condition;
			var scope = context.Scope;

			IEnumerable<DataDictionary> items;

			if(context.IsMultiple)
				items = (IEnumerable<DataDictionary>)context.Data;
			else
				items = new DataDictionary[] { (DataDictionary)context.Data };

			var info = MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var sqls = new List<KeyValuePair<string, DbParameter[]>>();
			var classInfo = CreateClassInfo("T", info);

			var setFormat = "{0}={{{1}}}";
			var addToSetFormat = "{0}={1} {2} {{{3}}}";
			var setNullFormat = "{0}=NULL";
			var updateformat = "UPDATE {0} SET {1} {2}";
			var tableName = classInfo.GetTableName();

			var whereValues = new object[0];
			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			string wheresql = string.Empty;

			if(condition != null)
			{
				var columns = GetConditionName(condition);
				var columnInofs = CreateColumnInfo(columns, classInfo);
				classInfo.SetJoinIndex(0);
				wheresql = condition.ToWhere(columnInofs, ref tableIndex, ref joinStartIndex, ref valueIndex, out whereValues);
			}

			foreach(var item in items)
			{
				if(item == null)
					continue;

				string[] members = this.ResolveScope(name, scope, item);

				if(members == null || members.Length == 0)
					throw new ArgumentNullException("members");

				Dictionary<PropertyNode, object> pks;
				var dic = GetColumnFromEntity(info, item, members, out pks);

				var tempValueIndex = valueIndex;

				if(condition == null)//condition为空则跟据主键修改
				{
					if(pks == null || pks.Count == 0)
						throw new ArgumentException("未设置Condition，也未找到主键");

					var newCondition = new ConditionCollection(ConditionCombination.And, pks.Select(p => new Condition(p.Key.Name, p.Value)));

					classInfo.Joins.Clear();
					var columnInfos = CreateColumnInfo(pks.Select(p => p.Key.Name), classInfo);
					classInfo.SetJoinIndex(0);

					wheresql = newCondition.ToWhere(columnInfos, ref tableIndex, ref joinStartIndex, ref tempValueIndex, out whereValues);
				}

				var temp = dic.Where(p => p.Value != null && !(p.Value is System.Linq.Expressions.Expression));
				var list = temp.Select((p, i) => string.Format(setFormat, CreateColumnInfo(p.Key.Field).ToColumn(), i + tempValueIndex)).ToList();
				var paramers = CreateParameters(0, whereValues).Concat(temp.Select((p, i) => CreateParameter(i + tempValueIndex, p.Value))).ToList();
				tempValueIndex += list.Count;

				var expressionValues = dic.Where(p =>
				{
					var flag = p.Value is BinaryExpression;
					if(!flag)
						return false;
					var expression = (BinaryExpression)p.Value;
					return expression.Left is MemberExpression && expression.Right is ConstantExpression;
				}).ToDictionary(p => p.Key, p => (BinaryExpression)p.Value);

				list.AddRange(expressionValues.Select((p, i) =>
				{
					var leftName = ((MemberExpression)p.Value.Left).Member.Name;
					var tempPropertyNode = info.PropertyNodeList.FirstOrDefault(pp => pp.Name.Equals(leftName, StringComparison.OrdinalIgnoreCase)) ?? new PropertyNode(leftName);
					return string.Format(addToSetFormat, CreateColumnInfo(p.Key.Field).ToColumn(), CreateColumnInfo(tempPropertyNode.Field).ToColumn(), p.Value.NodeType.ToSQL(), i + valueIndex);
				}));

				paramers.AddRange(expressionValues.Select((p, i) => CreateParameter(i + tempValueIndex, ((ConstantExpression)p.Value.Right).Value)));

				list.AddRange(dic.Where(p => p.Value == null).Select(p => string.Format(setNullFormat, CreateColumnInfo(p.Key.Field).ToColumn())));

				var sql = string.Format(updateformat, tableName, string.Join(",", list), wheresql);

				sqls.Add(new KeyValuePair<string, DbParameter[]>(sql, paramers.ToArray()));
			}

			var updateCount = 0;

			using(var transaction = new Zongsoft.Transactions.Transaction())
			{
				foreach(var sql in sqls)
				{
					updateCount += this.Executer.Execute(sql.Key, sql.Value);
				}

				transaction.Commit();
			}

			context.Count = updateCount;
		}
		#endregion

		#region 是否存在
		protected override void OnExists(DataExistenceContext context)
		{
			var name = context.Name;
			var condition = context.Condition;

			var info = this.MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var joinInfos = new Dictionary<string, Join>();
			var allColumnInfos = new Dictionary<string, ColumnInfo>();

			var allcolumns = GetConditionName(condition);

			var classInfo = CreateClassInfo("T", info);
			allColumnInfos = CreateColumnInfo(allcolumns, classInfo);
			classInfo.SetJoinIndex(0);

			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			object[] values;
			var whereSql = condition.ToWhere(allColumnInfos, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);

			var tempJoinInfos = new List<Join>();
			foreach(var item in allColumnInfos.Keys)
			{
				var columnInfo = allColumnInfos[item];
				if(columnInfo.Join != null && !tempJoinInfos.Contains(columnInfo.Join))
				{
					tempJoinInfos.Add(columnInfo.Join);
					tempJoinInfos.AddRange(columnInfo.Join.GetParent(p => tempJoinInfos.Contains(p)));
				}
			}

			var joinsql = string.Join(" ", tempJoinInfos.OrderBy(p => p.Target.AsIndex).Select(p => p.ToJoinSql(CreateColumnInfo)));

			var sql = string.Format("SELECT 0 FROM {0} {1} {2} LIMIT 0,1", classInfo.GetTableName(), joinsql, whereSql);

			var result = Executer.ExecuteScalar(sql, CreateParameters(0, values));
			context.Result = (result != null);
		}
		#endregion

		protected override void OnIncrement(DataIncrementContext context)
		{
			var name = context.Name;
			var member = context.Member;
			var condition = context.Condition;
			var interval = context.Interval;

			var info = MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var classInfo = CreateClassInfo("T", info);
			var tableName = classInfo.GetTableName();
			var columnInfo = CreateColumnInfo(member);

			var whereValues = new object[0];
			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			string wheresql = string.Empty;
			var columns = GetConditionName(condition);
			var columnInofs = CreateColumnInfo(columns, classInfo);
			classInfo.SetJoinIndex(0);
			wheresql = condition.ToWhere(columnInofs, ref tableIndex, ref joinStartIndex, ref valueIndex, out whereValues);
			var paramers = CreateParameters(0, whereValues);

			var sql = string.Format("UPDATE {0} SET {1}={1}+({2}) {3}",
									tableName, columnInfo.ToColumn(), interval, wheresql);

			context.Result = -1;

			using(var transaction = new Zongsoft.Transactions.Transaction())
			{
				if(this.Executer.Execute(sql, paramers) > 0)
				{
					sql = string.Format("SELECT {0} FROM {1} {2}", columnInfo.ToColumn(), tableName, wheresql);
					context.Result = Zongsoft.Common.Convert.ConvertValue<long>(this.Executer.ExecuteScalar(sql, paramers));
				}

				transaction.Commit();
			}
		}

		protected virtual Type GetEntityType(string name)
		{
			return MappingInfo.ClassNodeList.Where(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase)).Select(p => p.EntityType).FirstOrDefault();
		}
		#endregion

		#region 方法
		/// <summary>
		/// 
		/// </summary>
		/// <typeparam name="T">返回结果的类型,classInfo.ClassNode.EntityType这个类型只用来生成查询列</typeparam>
		/// <param name="table"></param>
		/// <param name="classInfo"></param>
		/// <returns></returns>
		internal IEnumerable<T> SetEntityValue<T>(IEnumerable<Dictionary<string, object>> table, ClassInfo classInfo)
		{
			var entityType = typeof(T);
			if(entityType == typeof(object))
				entityType = classInfo.ClassNode.EntityType;

			foreach(var row in table)
			{
				var values = row.Where(p => p.Key.StartsWith(classInfo.AsName + "_")).ToDictionary(p => p.Key.Substring(classInfo.AsName.Length + 1), p => p.Value);

				var currentClassInfo = classInfo;
				while(currentClassInfo.ClassNode.BaseClassNode != null && currentClassInfo.Joins != null)
				{
					var join = classInfo.Joins.FirstOrDefault(pp => pp.JoinInfo.Name.Equals(classInfo.ClassNode.BaseClassNode.Name, StringComparison.OrdinalIgnoreCase));
					if(join != null)
					{
						currentClassInfo = join.Target;
						var asName = join.Target.AsName;

						var temp = row.Where(p => p.Key.StartsWith(asName + "_")).ToDictionary(p => p.Key.Substring(asName.Length + 1), p => p.Value);
						foreach(var key in temp.Keys)
						{
							if(values.ContainsKey(key))
								values.Add(string.Format("{0}(base)", key), temp[key]);
							else
								values.Add(key, temp[key]);
						}
					}
					else
						break;
				}

				var entity = CreateEntity(entityType, values, classInfo.ClassNode);

				var flag = values == null || values.Count == 0 || values.All(p => p.Value is System.DBNull);
				if(!SetNavigationProperty(classInfo, entity, row) && flag)
					continue;

				yield return (T)entity;
			}
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="classInfo"></param>
		/// <param name="entity"></param>
		/// <param name="values"></param>
		/// <returns>返回值表示是否成功创建导航属性</returns>
		private bool SetNavigationProperty(ClassInfo classInfo, object entity, IDictionary<string, object> values)
		{
			var success = false;
			if(classInfo.Joins != null && classInfo.Joins.Count > 0)
			{
				var type = entity.GetType();
				foreach(var item in classInfo.Joins)
				{
					var dic = values.Where(p => p.Key.StartsWith(item.Target.AsName + "_")).ToDictionary(p => p.Key.Substring(item.Target.AsName.Length + 1), p => p.Value);

					if(type.IsDictionary())
					{
						SetNavigationProperty(item.Target, dic, values);
						((IDictionary)entity).Add(item.JoinInfo.Name, dic);
						success = true;
						continue;
					}

					if(classInfo.ClassNode.BaseClassNode!=null&&item.JoinInfo.Name == classInfo.ClassNode.BaseClassNode.Name)
					{
						SetNavigationProperty(item.Target, entity, values);
						continue;
					}

					var property = type.GetProperty(item.JoinInfo.Name);
					if(property == null)
						continue;
					//flag=true也要创建一个空实体，因为可能要创建这个空实体的导航属性
					var propertyValue = CreateEntity(property.PropertyType, dic, item.Target.ClassNode);

					if(propertyValue == null)
						continue;

					var flag = dic == null || dic.Count == 0 || dic.All(p => p.Value is System.DBNull);

					if(!SetNavigationProperty(item.Target, propertyValue, values) && flag)
						continue;

					property.SetValue(entity, propertyValue);
					success = true;
				}
			}

			return success;
		}

		protected object CreateEntity(Type entityType, IDictionary<string, object> propertyValues, ClassNode classNode)
		{
			if(entityType.IsDictionary())
				return propertyValues;

			KeyValuePair<System.Reflection.ParameterInfo[], System.Reflection.PropertyInfo[]> dicValue;
			System.Reflection.ParameterInfo[] cpinfo;
			System.Reflection.PropertyInfo[] properties;

			object[] instanceArgs = null;

			if(classNode != null)
			{
				var constructorPropertys = classNode.PropertyNodeList.Where(p => p.PassedIntoConstructor).ToList();

				if(!_typeDictionary.TryGetValue(entityType, out dicValue) || dicValue.Value == null)
				{
					cpinfo = entityType.GetConstructors().Where(p => p.IsPublic).Select(p => p.GetParameters()).FirstOrDefault(p => p.Length == constructorPropertys.Count);
					properties = entityType.GetProperties().OrderBy(p => p.Name).ToArray();
					_typeDictionary.TryAdd(entityType, new KeyValuePair<System.Reflection.ParameterInfo[], System.Reflection.PropertyInfo[]>(cpinfo, properties));
				}
				else
				{
					cpinfo = dicValue.Key;
					properties = dicValue.Value;
				}

				//如果构造函数需要参数，但不能提供参数时返回null;
				if(constructorPropertys.Count > 0 && (propertyValues == null || propertyValues.Count == 0 || propertyValues.All(p => p.Value == DBNull.Value)))
					return null;

				instanceArgs = new object[constructorPropertys.Count];
				constructorPropertys.ForEach(p =>
				{
					var tempValue = propertyValues.FirstOrDefault(pp => pp.Key.Equals(p.Field, StringComparison.OrdinalIgnoreCase));
					var args = cpinfo == null ? null : cpinfo.FirstOrDefault(pp => pp.Name.Equals(p.ParameterName, StringComparison.OrdinalIgnoreCase));
					if(args != null)
						instanceArgs[args.Position] = Zongsoft.Common.Convert.ConvertValue(tempValue.Value, args.ParameterType);
				});
			}
			else
			{
				properties = entityType.GetProperties().OrderBy(p => p.Name).ToArray();
			}

			object entity;

			if(entityType.IsValueType || instanceArgs == null || instanceArgs.Length == 0)
				entity = System.Activator.CreateInstance(entityType);
			else
				entity = System.Activator.CreateInstance(entityType, instanceArgs);

			if(propertyValues == null || propertyValues.Count == 0 || propertyValues.All(p => p.Value == DBNull.Value))
				return entity;

			foreach(var property in properties)
			{
				if(!property.CanWrite)
					continue;

				var isClass = property.PropertyType.IsClass && property.PropertyType != typeof(string) && !property.PropertyType.IsArray;
				if(isClass)
				{
					var tempDictionary = propertyValues.Where(p => p.Key.StartsWith(property.Name + ".")).ToDictionary(p => p.Key.Substring(property.Name.Length + 1), p => p.Value);

					if(tempDictionary.Count > 0)
					{
						property.SetValue(entity, CreateEntity(property.PropertyType, tempDictionary, null));
					}

					continue;
				}

				var propertyInfo = classNode == null ? null : classNode.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(property.Name, StringComparison.CurrentCultureIgnoreCase));
				if(propertyInfo != null && string.IsNullOrEmpty(propertyInfo.Field))
					continue;

				var tempValue = propertyValues.FirstOrDefault(p => p.Key.Equals(propertyInfo != null ? propertyInfo.Field : property.Name, StringComparison.OrdinalIgnoreCase));
				if(tempValue.Value == null || tempValue.Value is System.DBNull)
					continue;

				var propertyValue = Zongsoft.Common.Convert.ConvertValue(tempValue.Value, property.PropertyType);
				if(propertyValue == null)
					continue;

				property.SetValue(entity, propertyValue, null);
			}

			return entity;
		}

		protected Dictionary<PropertyNode, object> GetColumnFromEntity(ClassNode classNode, object entity, string[] members, out Dictionary<PropertyNode, object> pks)
		{
			var properties = new Dictionary<PropertyNode, object>();
			pks = new Dictionary<PropertyNode, object>();

			if(entity is IDictionary<string, object>)
			{
				var dic = (IDictionary<string, object>)entity;
				foreach(var key in dic.Keys)
				{
					var propertyNo = classNode.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(key, StringComparison.OrdinalIgnoreCase)) ?? new PropertyNode(key);
					if(propertyNo.Ignored)
						continue;

					var value = dic[key];
					if(propertyNo.IsKey)
						pks.Add(propertyNo, value);

					if(members != null && !members.Contains(key, StringComparer.OrdinalIgnoreCase))
						continue;

					properties.Add(propertyNo, value);
				}
			}
			else
			{
				Type type = entity.GetType();
				foreach(var property in type.GetProperties())
				{
					var propertyNo = classNode.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(property.Name, StringComparison.OrdinalIgnoreCase)) ?? new PropertyNode(property.Name);
					if(propertyNo.Ignored)
						continue;

					object value = null;

					if(property.PropertyType.IsScalarType() || typeof(Expression).IsAssignableFrom(property.PropertyType))
					{
						value = property.GetValue(entity, null);

						if(propertyNo.IsKey)
							pks.Add(propertyNo, value);

						if(members == null || members.Contains(property.Name, StringComparer.OrdinalIgnoreCase))
							properties.Add(propertyNo, value);
					}
				}
			}

			return properties;
		}

		public DbParameter[] CreateParameters(int startIndex, object[] values)
		{
			return values.Select((p, i) => CreateParameter(startIndex + i, p)).ToArray();
		}
		#endregion

		#region 抽像方法
		internal abstract DbParameter CreateParameter(int index, object value, string dbType = null, string name = null, bool isOutPut = false, bool isInOutPut = false, int? size = null, bool isProcedure = false);
		#endregion

		#region 私有方法
		private string[] GetConditionName(ICondition condition)
		{
			if(condition is IConditional)
				condition = ((IConditional)condition).ToConditions();
				
			if(condition == null)
				return null;

			if(condition is Condition)
			{
				var where = (Condition)condition;
				return new[] { where.Name };
			}
			else
			{
				var list = new List<string>();
				foreach(var item in (ConditionCollection)condition)
				{
					string[] conditionName;

					if(item != null && (conditionName = GetConditionName(item)) != null)
					{
						list.AddRange(conditionName);
					}
				}

				return list.ToArray();
			}
		}

		/// <summary>
		/// 预处理
		/// 在关联多张表时，父关联如果是left join并且子关联有inner join时要添加父关联的join on 条件
		/// </summary>
		/// <param name="joinList"></param>
		private void Pretreatment(List<Join> joinList)
		{
			var whereformat = "WHERE {0}={1}";
			var paging = new Paging(1, 1);
			foreach(var item in joinList)
			{
				if(item.JoinInfo.Type == JoinType.Inner && ParentHasLeftJoin(item))
				{
					var tempClassInfo = CreateClassInfo("TT", item.Target.ClassNode);
					item.Parent.AddJoinWhere(string.Join(" AND ", item.JoinInfo.Member.Select(p =>
					{
						var where = string.Format(whereformat, CreateColumnInfo(p.Value.Field).ToColumn(tempClassInfo.AsName), CreateColumnInfo(p.Key.Field).ToColumn(item.Host.AsName));
						var subparameter = new CreateSelectSqlParameter(true);
						subparameter.Info = tempClassInfo;
						subparameter.Columns = new List<ColumnInfo>();
						subparameter.Columns.Add(CreateColumnInfo("0"));
						subparameter.Where = where;
						subparameter.Paging = paging;

						return string.Format("EXISTS({0})", this.CreateSelectSql(subparameter));
					})));

					item.ChangeMode(JoinType.Left);
				}
				continue;
			}
		}

		private bool ParentHasLeftJoin(Join join)
		{
			if(join.Parent == null)
				return false;

			return join.Parent.JoinInfo.Type == JoinType.Left || ParentHasLeftJoin(join.Parent);
		}

		private string[] ResolveScope(string entityName, string scope, Type entityType)
		{
			if(string.IsNullOrWhiteSpace(entityName))
				throw new ArgumentNullException("entityName");

			var isWeakType = entityType != null && (typeof(IDictionary).IsAssignableFrom(entityType) || Zongsoft.Common.TypeExtension.IsAssignableFrom(typeof(IDictionary<,>), entityType));

			if(entityType == null || isWeakType)
				entityType = this.GetEntityType(entityName);

			var entityDescriptor = _entityCache.GetOrAdd(entityType, type => new EntityDesciptior(this, entityName, type));
			return this.ResolveScope(null, entityDescriptor, scope, isWeakType).ToArray();
		}

		private string[] ResolveScope(string entityName, string scope, DataDictionary data)
		{
			if(string.IsNullOrWhiteSpace(entityName))
				throw new ArgumentNullException("entityName");

			var entityType = data.Data.GetType();
			var isWeakType = typeof(IDictionary).IsAssignableFrom(entityType) || Zongsoft.Common.TypeExtension.IsAssignableFrom(typeof(IDictionary<,>), entityType);

			if(entityType == null || isWeakType)
				entityType = this.GetEntityType(entityName);

			var entityDescriptor = _entityCache.GetOrAdd(entityType, type => new EntityDesciptior(this, entityName, type));
			return this.ResolveScope(data, entityDescriptor, scope, isWeakType).ToArray();
		}

		private HashSet<string> ResolveScope(DataDictionary data, EntityDesciptior entity, string scope, bool isWeakType)
		{
			HashSet<string> result;

			if(data == null)
				result = new HashSet<string>(entity.Properties.Where(p => p.IsScalarType).Select(p => p.PropertyName), StringComparer.OrdinalIgnoreCase);
			else
				result = new HashSet<string>(data.Where(p => p.Value == null || this.IsScalarType(p.Value.GetType())).Select(p => p.Key), StringComparer.OrdinalIgnoreCase);

			if(string.IsNullOrWhiteSpace(scope))
				return result;

			var members = scope.Split(',', ';');

			for(int i = 0; i < members.Length; i++)
			{
				var member = members[i].Trim();

				if(member.Length == 0)
					continue;

				switch(member[0])
				{
					case '-':
					case '!':
						if(member.Length > 1)
							result.Remove(member.Substring(1));
						else
							result.Clear();

						break;
					case '*':
						if(member.Length != 1)
							throw new ArgumentException("scope");

						result.UnionWith(entity.Properties.SelectMany(p =>
						{
							if(p.IsScalarType)
								return new string[] { p.PropertyName };

							var list = new List<string>();
							this.GetComplexPropertyMembers(entity.EntityName, p.PropertyName, p.PropertyType, list, new HashSet<Type>(new Type[] { p.PropertyType }));
							return list.ToArray();
						}));

						break;
					default:
						if((member[0] >= 'A' && member[0] <= 'Z') || (member[0] >= 'a' && member[0] <= 'z') || member[0] == '_')
						{
							EntityPropertyDescriptor property = null;

							if(member.Contains("."))
							{
								var navigationProperty = GetNavigationProperty(member, entity.EntityType, isWeakType);

								if(navigationProperty != null)
									property = new EntityPropertyDescriptor(member, navigationProperty.PropertyType, this.IsScalarType(navigationProperty.PropertyType));
							}
							else
							{
								property = entity.Properties.FirstOrDefault(p => string.Equals(p.PropertyName, member, StringComparison.OrdinalIgnoreCase));
							}

							if(property == null)
							{
								if(isWeakType)
								{
									result.Add(member);
									continue;
								}

								throw new ArgumentException(string.Format("The '{0}' property is not exists in the '{1}' entity.", member, entity.EntityName));
							}

							if(property.IsScalarType)
								result.Add(member);
							else
							{
								var list = new List<string>();
								this.GetComplexPropertyMembers(entity.EntityName, property.PropertyName, property.PropertyType, list, null);
								result.UnionWith(list);
							}
						}
						else
						{
							throw new ArgumentException(string.Format("Invalid '{0}' member in the '{1}' scope.", member, scope));
						}

						break;
				}
			}

			return result;
		}

		private PropertyDescriptor GetNavigationProperty(string path, Type type, bool isWeakType)
		{
			if(string.IsNullOrWhiteSpace(path))
				return null;

			var parts = path.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
			PropertyDescriptor property = null;

			foreach(var part in parts)
			{
				property = TypeDescriptor.GetProperties(type).Find(part, true);

				if(property == null)
					throw new ArgumentException(string.Format("The '{0}' member is not existed in the '{1}' type, the original text is '{2}'.", part, type.FullName, path));

				type = property.PropertyType;
			}

			return property;
		}

		private void GetComplexPropertyMembers(string entityName, string memberPrefix, Type memberType, ICollection<string> collection, HashSet<Type> recursiveStack)
		{
			foreach(PropertyDescriptor property in TypeDescriptor.GetProperties(memberType))
			{
				if(this.IsScalarType(property.PropertyType))
					collection.Add(memberPrefix + "." + property.Name);
				else if(recursiveStack != null && !recursiveStack.Contains(property.PropertyType))
				{
					recursiveStack.Add(property.PropertyType);
					GetComplexPropertyMembers(entityName, memberPrefix + "." + property.Name, property.PropertyType, collection, recursiveStack);
				}
			}
		}
		#endregion

		#region 嵌套子类
		private class EntityDesciptior
		{
			public readonly string EntityName;
			public readonly Type EntityType;

			private readonly ObjectAccess _dataAccess;
			private EntityPropertyDescriptor[] _properties;

			public EntityDesciptior(ObjectAccess dataAccess, string entityName, Type entityType)
			{
				_dataAccess = dataAccess;

				if(string.IsNullOrWhiteSpace(entityName))
					throw new ArgumentNullException("entityName");

				this.EntityName = entityName;
				this.EntityType = entityType;
			}

			public EntityPropertyDescriptor[] Properties
			{
				get
				{
					if(_properties == null)
					{
						lock (this)
						{
							if(_properties == null)
								this.InitializeProperties(this.EntityType);
						}
					}

					return _properties;
				}
			}

			private void InitializeProperties(Type type)
			{
				var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

				_properties = new EntityPropertyDescriptor[properties.Length];

				for(int i = 0; i < properties.Length; i++)
				{
					_properties[i] = new EntityPropertyDescriptor(properties[i].Name, properties[i].PropertyType, _dataAccess.IsScalarType(properties[i].PropertyType));
				}
			}
		}

		private class EntityPropertyDescriptor
		{
			public readonly string PropertyName;
			public readonly Type PropertyType;
			public readonly bool IsScalarType;

			public EntityPropertyDescriptor(string propertyName, Type propertyType, bool isScalarType)
			{
				this.PropertyName = propertyName;
				this.PropertyType = propertyType;
				this.IsScalarType = isScalarType;
			}
		}
		#endregion
	}
}
