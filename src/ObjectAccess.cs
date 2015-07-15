/*
 * Authors:
 *   喻星(Xing Yu) <491718907@qq.com>
 *
 * Copyright (C) 2015 Automao Network Co., Ltd. <http://www.zongsoft.com>
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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;

using Zongsoft.Data;
using Zongsoft.ComponentModel;
using Zongsoft.Transactions;

using Automao.Data.Options.Configuration;

namespace Automao.Data
{
	public abstract class ObjectAccess : DataAccessBase, IEnlistment
	{
		#region 字段
		private SqlExecuter _executer;
		private bool _caseSensitive;
		private DbProviderFactory _providerFactory;
		private List<ClassInfo> _classInfoList;
		#endregion

		#region 构造函数
		/// <summary>
		/// 
		/// </summary>
		/// <param name="caseSensitive">区分大小写</param>
		public ObjectAccess(bool caseSensitive, DbProviderFactory providerFactory)
		{
			_caseSensitive = caseSensitive;
			_providerFactory = providerFactory;
		}
		#endregion

		#region 属性
		public GeneralOption Option
		{
			get;
			set;
		}

		protected List<ClassInfo> ClassInfoList
		{
			get
			{
				if(_classInfoList == null)
					System.Threading.Interlocked.CompareExchange(ref _classInfoList,
						MappingInfo.CreateClassInfo(Option.Mappings.Select(p => ((Mapping)p).Path).ToArray(), Option.MappingFileName), null);
				return _classInfoList;
			}
		}

		internal SqlExecuter Executer
		{
			get
			{
				if(_executer == null)
					Interlocked.CompareExchange(ref _executer, new SqlExecuter(this)
					{
						DbProviderFactory = _providerFactory,
						ConnectionString = Option.ConnectionString
					}, null);
				return _executer;
			}
		}
		#endregion

		#region Base成员
		#region 查询
		protected override IEnumerable<T> Select<T>(string name, ICondition condition, string[] members, Paging paging, Grouping grouping, Sorting[] sorting)
		{
			if(string.IsNullOrEmpty(name))
				name = typeof(T).Name;

			var classInfo = ClassInfoList.FirstOrDefault(p => p.ClassName.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(classInfo == null)
				throw new Exception(string.Format("未找到{0}对应的mapping节点", name));

			object[] values;
			int joinStartIndex;
			EachCondition(condition, out joinStartIndex, out values);

			int valueCount = values.Length;
			var rootJoin = new JoinInfo(null, "", values.Length > 0 ? "T1" : "T", classInfo, null);
			string countSql;
			var sql = CreateSelectSql(rootJoin, classInfo, condition, members, paging, grouping, sorting, ref joinStartIndex, ref valueCount, out countSql, out values);

			if(!string.IsNullOrEmpty(countSql))
			{
				var tablevalues = this.Executer.Select(countSql, CreateParameters(0, values));

				if(tablevalues != null)
				{
					var item = tablevalues.FirstOrDefault();
					paging.TotalCount = Convert.ToInt32(item.Values.First());
				}
			}

			return new ObjectAccessResult<T>(joinStartIndex, values, sql, () =>
			{

				var tablevalues = this.Executer.Select(sql, this.CreateParameters(0, values));

				var result = this.SetEntityValue<T>(tablevalues, rootJoin);

				return result;
			});
		}

		internal string CreateSelectSql(JoinInfo rootJoin, ClassInfo classInfo, ICondition condition, string[] members, Paging paging, Grouping grouping, Sorting[] sorting, ref int joinStartIndex, ref int valueCount, out string countSql, out object[] values)
		{
			#region 构建所有列
			var joinInfos = new Dictionary<string, JoinInfo>();
			var allColumnInfos = new Dictionary<string, ColumnInfo>();
			var selectColumnInfos = new Dictionary<string, ColumnInfo>();

			var conditionNames = GetConditionName(condition) ?? new string[0];
			IEnumerable<string> allColumns = conditionNames;

			if(grouping != null)
			{
				allColumns = allColumns.Concat(grouping.Members);
				if(grouping.Condition != null)
					allColumns = allColumns.Concat(GetConditionName(grouping.Condition));
			}

			if(sorting != null)
			{
				foreach(var item in sorting)
				{
					allColumns = allColumns.Concat(item.Members);
				}
			}

			allColumns = allColumns.Concat(members);


			foreach(var item in allColumns)
			{
				if(allColumnInfos.ContainsKey(item))
					continue;

				var columnInfo = new ColumnInfo(joinStartIndex, item, classInfo, joinInfos, rootJoin);
				allColumnInfos.Add(item, columnInfo);
			}

			joinStartIndex += joinInfos.Count;
			#endregion

			Func<ColumnInfo, bool> predicate = (p =>
			{
				var columnInfo = p;
				if(columnInfo.PropertyInfo != null)
					return !string.IsNullOrEmpty(columnInfo.PropertyInfo.TableColumnName);//排除列名为空的字段
				return !columnInfo.ClassInfo.PropertyInfoList.Where(pp => pp.IsFKColumn).Any(pp => pp.SetClassPropertyName == columnInfo.Field);//排除导航属性
			});

			#region 要查询的列
			var selectMembers = members.Where(p => grouping == null || p.Contains('(') && !grouping.Members.Contains(p));
			if(grouping != null)
				selectMembers = selectMembers.Concat(grouping.Members);

			var columns = string.Join(",", selectMembers.Select(p => allColumnInfos[p]).Where(predicate).Select(p => p.ToSelectColumn(_caseSensitive)));
			#endregion

			#region where
			int startFormatIndex = valueCount;

			var where = "WHERE {0}".FormatWhere(condition, allColumnInfos, _caseSensitive, ref startFormatIndex, out values);
			#endregion

			#region join
			var tempJoinInfos = new List<JoinInfo>();
			var tempList = conditionNames.Concat(selectMembers);
			if(grouping != null && grouping.Condition != null)
				tempList = tempList.Concat(GetConditionName(grouping.Condition));
			foreach(var item in tempList.ToArray())
			{
				var columnInfo = allColumnInfos[item];
				if(columnInfo.JoinInfo != rootJoin && !tempJoinInfos.Contains(columnInfo.JoinInfo))
				{
					tempJoinInfos.Add(columnInfo.JoinInfo);
					tempJoinInfos.AddRange(columnInfo.JoinInfo.GetParent(p => p == rootJoin || tempJoinInfos.Contains(p)));
				}
			}
			var join = string.Join(" ", tempJoinInfos.OrderBy(p => p.TableEx).Select(p => p.ToJoinSql(_caseSensitive)));
			#endregion

			#region grouping
			string having = string.Empty;
			string group = string.Empty;
			string groupedSelectColumns = string.Empty;
			string groupedJoin = string.Empty;
			string newTableNameEx = rootJoin.TableEx + '1';
			if(grouping != null)
			{
				var groupColumnInfos = grouping.Members.Select(p => allColumnInfos[p]).ToArray();
				group = string.Format("GROUP BY {0}", string.Join(",", groupColumnInfos.Select(p => p.ToColumn(_caseSensitive))));

				var groupedSelectMembers = members.Where(p => !p.Contains('(') && !grouping.Members.Contains(p));
				groupedSelectColumns = string.Join(",", groupedSelectMembers.Select(p => allColumnInfos[p]).Where(predicate).Select(p => p.ToSelectColumn(_caseSensitive)));

				tempJoinInfos = new List<JoinInfo>();
				foreach(var item in groupedSelectMembers)
				{
					var columnInfo = allColumnInfos[item];
					if(columnInfo.JoinInfo != rootJoin && !tempJoinInfos.Contains(columnInfo.JoinInfo))
					{
						tempJoinInfos.Add(columnInfo.JoinInfo);
						tempJoinInfos.AddRange(columnInfo.JoinInfo.GetParent(p => groupColumnInfos.Any(pp => pp.JoinInfo == p) || tempJoinInfos.Contains(p)));
					}
				}

				groupedJoin = string.Join(" ", tempJoinInfos.OrderBy(p => p.TableEx).Select(p =>
				{
					var groupColumnInfo = groupColumnInfos.Where(gc => gc.JoinInfo == p.Parent).ToArray();
					if(groupColumnInfo.Any())
					{
						var dic = p.ParentJoinColumn.ToDictionary(c => groupColumnInfo.FirstOrDefault(gc => gc.Field == c.ClassPropertyName).GetColumnEx(_caseSensitive),
							c => c.TableColumnName);

						return JoinInfo.CreatJoinSql(_caseSensitive, p.Parent.ParentJoinColumn.Any(c => c.Nullable), p.ClassInfo.TableName, p.TableEx, newTableNameEx, dic);
					}
					return p.ToJoinSql(_caseSensitive);
				}));

				if(grouping.Condition != null)
				{
					object[] tempValues;
					having = "HAVING {0}".FormatWhere(grouping.Condition, allColumnInfos, _caseSensitive, ref startFormatIndex, out tempValues);
					values = values.Concat(tempValues).ToArray();
				}
			}
			#endregion

			var orderby = sorting == null || sorting.Length == 0 ? "" : string.Format("ORDER BY {0}", string.Join(",", sorting.Select(p => p.Parse(allColumnInfos, _caseSensitive))));

			var sql = CreateSelectSql(classInfo.TableName, rootJoin.TableEx, newTableNameEx, columns, where, join,
				group, having, groupedSelectColumns, groupedJoin, orderby, paging);

			if(paging != null && paging.TotalCount == 0)
			{
				countSql = CreateSelectSql(classInfo.TableName, rootJoin.TableEx, newTableNameEx, grouping == null ? "COUNT(0)" : columns, where, join,
					group, having, grouping == null ? null : "COUNT(0)", groupedJoin, null, null);
			}
			else
				countSql = string.Empty;

			return sql;
		}

		protected abstract string CreateSelectSql(string tableName, string tableNameEx, string newTableNameEx, string columns, string where, string join, string group, string having, string groupedSelectColumns, string groupedJoin, string orderby, Paging paging);
		#endregion

		#region Count
		protected override int Count(string name, ICondition condition, string[] includes)
		{
			if(string.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			var info = this.ClassInfoList.FirstOrDefault(p => p.ClassName.Equals(name, StringComparison.CurrentCultureIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var joinInfos = new Dictionary<string, JoinInfo>();
			var allColumnInfos = new Dictionary<string, ColumnInfo>();
			var rootJoin = new JoinInfo(null, "", "T", info, null);
			var conditionNames = GetConditionName(condition);
			var allcolumns = conditionNames.Concat(includes);
			foreach(var item in allcolumns)
			{
				if(allColumnInfos.ContainsKey(item))
					continue;

				var columnInfo = new ColumnInfo(0, item, info, joinInfos, rootJoin);
				allColumnInfos.Add(item, columnInfo);
			}

			int startFormatIndex = 0;
			object[] values;
			var whereSql = "WHERE {0}".FormatWhere(condition, allColumnInfos, _caseSensitive, ref startFormatIndex, out values);

			var tempJoinInfos = new List<JoinInfo>();
			foreach(var item in allColumnInfos.Keys)
			{
				var columnInfo = allColumnInfos[item];
				if(columnInfo.JoinInfo != rootJoin && !tempJoinInfos.Contains(columnInfo.JoinInfo))
				{
					tempJoinInfos.Add(columnInfo.JoinInfo);
					tempJoinInfos.AddRange(columnInfo.JoinInfo.GetParent(p => p == rootJoin || tempJoinInfos.Contains(p)));
				}
			}
			var joinsql = string.Join(" ", tempJoinInfos.OrderBy(p => p.TableEx).Select(p => p.ToJoinSql(_caseSensitive)));

			var format = _caseSensitive ? "SELECT {0} FROM \"{1}\" T {2} {3}" : "SELECT {0} FROM {1} T {2} {3}";
			string countSql;
			if(includes == null || includes.Length == 0)
				countSql = "COUNT(0)";
			else if(includes.Length == 1)
				countSql = string.Format("COUNT({0})", allColumnInfos[includes[0]].ToColumn(_caseSensitive));
			else
				countSql = string.Format("COUNT(CONCAT({0}))", string.Join(",", includes.Select(p => allColumnInfos[p]).Select(p => p.ToColumn(_caseSensitive))));

			var sql = string.Format(format, countSql, info.TableName, joinsql, whereSql);

			var result = Executer.ExecuteScalar(sql, CreateParameters(0, values));
			return int.Parse(result.ToString());
		}
		#endregion

		#region 删除
		protected override int Delete(string name, ICondition condition, string[] cascades)
		{
			if(cascades != null && cascades.Length > 0)
				throw new NotSupportedException();

			if(string.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			var info = this.ClassInfoList.FirstOrDefault(p => p.ClassName.Equals(name, StringComparison.CurrentCultureIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var conditionNames = GetConditionName(condition).Where(p => p.IndexOf('.') > 0).ToDictionary(p => p, p => p.Substring(0, p.LastIndexOf('.')));

			var values = new object[0];
			var formatStartIndex = 0;
			var whereSql = "WHERE {0}".FormatWhere(condition,
				(Func<string, Tuple<ClassInfo, string>>)(column => new Tuple<ClassInfo, string>(info, null)), _caseSensitive, ref formatStartIndex, out values);

			var format = _caseSensitive ? "DELETE FROM \"{0}\" {1}" : "DELETE FROM {0} {1}";
			var sql = string.Format(format, info.TableName, whereSql);
			return Executer.Execute(sql, CreateParameters(0, values));
		}
		#endregion

		#region 执行
		public override object Execute(string name, IDictionary<string, object> inParameters, out IDictionary<string, object> outParameters)
		{
			var classInfo = this.ClassInfoList.FirstOrDefault(p => p.ClassName.Equals(name, StringComparison.OrdinalIgnoreCase));

			if(classInfo == null)
			{
				outParameters = new Dictionary<string, object>();
				return null;
			}

			var procedureInfo = this.ClassInfoList.FirstOrDefault(p => p.ClassName.Equals(classInfo.TableName, StringComparison.OrdinalIgnoreCase));
			if(procedureInfo == null)
				procedureInfo = classInfo;

			var outPropertyInfos = procedureInfo.PropertyInfoList.Where(p => p.IsOutPutParamer);

			Dictionary<string, object> dic;
			var paramers = inParameters.Where(p => p.Value != null).Select((p, i) =>
			{
				var parameterName = p.Key;
				var dbType = "";
				bool isInOutPut = false;
				int? size = null;
				if(procedureInfo != null)
				{
					var item = procedureInfo.PropertyInfoList.FirstOrDefault(pp => pp.ClassPropertyName.Equals(p.Key, StringComparison.CurrentCultureIgnoreCase));
					if(item != null)
					{
						parameterName = item.TableColumnName;
						dbType = item.DbType;
						isInOutPut = item.IsOutPutParamer;
						size = item.Size;
					}
				}
				return CreateParameter(i, p.Value, dbType, parameterName, false, isInOutPut, size, true);
			}).ToList();

			paramers.AddRange(procedureInfo.PropertyInfoList.Where(p => !inParameters.ContainsKey(p.ClassPropertyName) && !p.PassedIntoConstructor).Select((p, i) => CreateParameter(paramers.Count + i, null, p.DbType, p.TableColumnName, p.IsOutPutParamer, false, p.Size, true)).ToArray());

			var tablevalues = Executer.ExecuteProcedure(procedureInfo.TableName, paramers.ToArray(), out dic);

			outParameters = dic.ToDictionary(p => outPropertyInfos.FirstOrDefault(pp => pp.TableColumnName == p.Key).ClassPropertyName, p => p.Value);

			return tablevalues.Select(p => CreateEntity<object>(classInfo.EntityType, p, classInfo));
		}
		#endregion

		#region 新增
		protected override int Insert<T>(string name, IEnumerable<T> entities, string[] includes)
		{
			if(!entities.GetEnumerator().MoveNext())
				return 0;

			if(string.IsNullOrEmpty(name))
				name = typeof(T).Name;

			List<string> pkColumnNames = new List<string>();

			var insertCount = 0;
			var sqls = new List<KeyValuePair<string, DbParameter[]>>();

			var insertformat = _caseSensitive ? "INSERT INTO \"{0}\"({1}) VALUES({2})" : "INSERT INTO {0}({1}) VALUES({2})";
			var columnformat = _caseSensitive ? "\"{0}\"" : "{0}";
			foreach(var item in entities)
			{
				var dic = GetColumnFromEntity(name, item).Where(p => p.Value != null && includes.Contains(p.Key.ClassPropertyName, StringComparer.OrdinalIgnoreCase)).ToDictionary(p => p.Key, p => p.Value);
				if(pkColumnNames == null)
				{
					pkColumnNames = dic.Where(p => p.Key.IsPKColumn).Select(p => p.Key.TableColumnName).OrderBy(p => p).ToList();
				}

				var sql = string.Format(insertformat, name,
					string.Join(",", dic.Keys.Select(p => string.Format(columnformat, p.TableColumnName))),
					string.Join(",", dic.Select((p, i) => string.Format("{{{0}}}", i)))
				);

				var paramers = dic.Select((p, i) => CreateParameter(i, p.Value, p.Key.DbType, size: p.Key.Size)).ToArray();

				sqls.Add(new KeyValuePair<string, DbParameter[]>(sql, paramers));

			}

			foreach(var item in sqls)
			{
				var count = Executer.Execute(item.Key, item.Value);
				if(count > 0)
					insertCount++;
			}

			return insertCount;
		}
		#endregion

		#region 修改
		protected override int Update<T>(string name, IEnumerable<T> entities, ICondition condition, string[] members)
		{
			if(string.IsNullOrEmpty(name))
				name = typeof(T).Name;

			if(members == null || members.Length == 0)
				throw new ArgumentNullException("members");

			var info = ClassInfoList.FirstOrDefault(p => p.ClassName.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var sqls = new List<KeyValuePair<string, DbParameter[]>>();
			var tuple = new Tuple<ClassInfo, string>(info, "T");

			var columnformat = _caseSensitive ? "\"{0}\"={{{1}}}" : "{0}={{{1}}}";
			var nullcolumnformat = _caseSensitive ? "\"{0}\"=NULL" : "{0}=NULL";
			var updateformat = _caseSensitive ? "UPDATE \"{0}\" T SET {1} {2}" : "UPDATE {0} T SET {1} {2}";

			foreach(var item in entities)
			{
				var dic = GetColumnFromEntity(name, item, members);

				var temp = dic.Where(p => p.Value != null);
				var list = temp.Select((p, i) => string.Format(columnformat, p.Key.TableColumnName, i)).ToList();
				var paramers = temp.Select((p, i) => CreateParameter(i, p.Value, p.Key.DbType, size: p.Key.Size)).ToList();

				list.AddRange(dic.Where(p => p.Value == null).Select(p => string.Format(nullcolumnformat, p.Key.TableColumnName)));

				var values = new object[0];
				var formatStartIndex = paramers.Count;
				var wheresql = "WHERE {0}".FormatWhere(condition, column => tuple, _caseSensitive, ref formatStartIndex, out values);
				paramers.AddRange(CreateParameters(paramers.Count, values));

				var sql = string.Format(updateformat, name, string.Join(",", list), wheresql);

				sqls.Add(new KeyValuePair<string, DbParameter[]>(sql, paramers.ToArray()));
			}

			var updateCount = 0;

			foreach(var sql in sqls)
			{
				updateCount += Executer.Execute(sql.Key, sql.Value);
			}

			return updateCount;
		}
		#endregion

		protected override Type GetEntityType(string name)
		{
			return ClassInfoList.Where(p => p.ClassName.Equals(name, StringComparison.OrdinalIgnoreCase)).Select(p => p.EntityType).FirstOrDefault();
		}
		#endregion

		#region IEnlistment成员
		public void OnEnlist(EnlistmentContext context)
		{
			throw new NotImplementedException();
		}
		#endregion

		#region 方法
		internal IEnumerable<T> SetEntityValue<T>(IEnumerable<Dictionary<string, object>> table, JoinInfo rootInfo)
		{
			foreach(var row in table)
			{
				var values = row.Where(p => p.Key.StartsWith(rootInfo.TableEx + "_")).ToDictionary(p => p.Key.Substring(rootInfo.TableEx.Length + 1), p => p.Value);
				var entityType = rootInfo.ClassInfo.EntityType;
				var entity = CreateEntity<T>(entityType, values, rootInfo.ClassInfo);

				SetNavigationProperty(rootInfo, entity, row);

				yield return entity;
			}
		}

		private void SetNavigationProperty(JoinInfo joinInfo, object entity, Dictionary<string, object> values)
		{
			if(joinInfo.Children != null)
			{
				var type = entity.GetType();
				foreach(var item in joinInfo.Children)
				{
					var property = type.GetProperty(item.Property);

					var dic = values.Where(p => p.Key.StartsWith(item.TableEx + "_")).ToDictionary(p => p.Key.Substring(item.TableEx.Length + 1), p => p.Value);
					if(dic == null || dic.Count == 0)
						continue;

					var value = CreateEntity<object>(item.ClassInfo.EntityType, dic, item.ClassInfo);

					SetNavigationProperty(item, value, values);

					property.SetValue(entity, value);
				}
			}
		}

		protected T CreateEntity<T>(Type entityType, Dictionary<string, object> propertyValues, ClassInfo info)
		{
			//获取当前类型公共构造函数中参数最少的构造函数的参数集合
			var cpinfo = entityType.GetConstructors().Where(p => p.IsPublic).Select(p => p.GetParameters()).OrderBy(p => p.Length).FirstOrDefault();
			var instanceArgs = new object[cpinfo.Length];
			if(info != null)
			{
				info.PropertyInfoList.Where(p => p.PassedIntoConstructor).ToList().ForEach(p =>
				{
					var tempValue = propertyValues.FirstOrDefault(pp => pp.Key.Equals(p.TableColumnName, StringComparison.OrdinalIgnoreCase));
					var args = cpinfo == null ? null : cpinfo.FirstOrDefault(pp => pp.Name == p.ConstructorName);
					if(args != null)
						instanceArgs[args.Position] = Zongsoft.Common.Convert.ConvertValue(tempValue.Value, args.ParameterType);
				});
			}

			T entity = default(T);

			if(entity == null)
			{
				if(instanceArgs.Length == 0 && typeof(T) != typeof(object))
					entity = Activator.CreateInstance<T>();
				else
					entity = (T)Activator.CreateInstance(entityType, instanceArgs);
			}

			var properties = entityType.GetProperties();

			foreach(var property in properties.OrderBy(p => p.Name))
			{
				if(!property.CanWrite)
					continue;
				var isClass = property.PropertyType.IsClass && property.PropertyType != typeof(string) && !property.PropertyType.IsArray;
				if(isClass)
					continue;

				var propertyInfo = info == null ? null : info.PropertyInfoList.FirstOrDefault(p => p.ClassPropertyName.Equals(property.Name, StringComparison.CurrentCultureIgnoreCase));
				if(propertyInfo != null && string.IsNullOrEmpty(propertyInfo.TableColumnName))
					continue;

				var tempValue = propertyValues.FirstOrDefault(p => p.Key.Equals(propertyInfo != null ? propertyInfo.TableColumnName : property.Name, StringComparison.OrdinalIgnoreCase));
				if(tempValue.Value == null || tempValue.Value is System.DBNull)
					continue;

				var propertyValue = Zongsoft.Common.Convert.ConvertValue(tempValue.Value, property.PropertyType);
				if(propertyValue == null)
					continue;

				property.SetValue(entity, propertyValue, null);
			}
			return entity;
		}

		protected Dictionary<ClassPropertyInfo, object> GetColumnFromEntity(string name, object entity, string[] members = null)
		{
			var info = this.ClassInfoList.FirstOrDefault(p => p.ClassName.Equals(name, StringComparison.CurrentCultureIgnoreCase));

			IDictionary<string, object> properties;

			if(entity is IDictionary<string, object>)
				properties = (IDictionary<string, object>)entity;
			else
			{
				Type type = entity.GetType();
				properties = type.GetProperties().Where(p => (p.PropertyType.IsValueType || p.PropertyType == typeof(string) || p.PropertyType == typeof(byte[]))).ToDictionary(p => p.Name, p => p.GetValue(entity, null));
			}

			var list = properties.Where(p => members == null || members.Contains(p.Key, StringComparer.OrdinalIgnoreCase));

			if(info == null)
			{
				return list.ToDictionary(p => new ClassPropertyInfo
				{
					ClassPropertyName = p.Key,
					TableColumnName = p.Key
				}, p => p.Value);
			}

			return list.Select(p =>
			{
				var item = info.PropertyInfoList.FirstOrDefault(pp => pp.ClassPropertyName.Equals(p.Key, StringComparison.CurrentCultureIgnoreCase));
				if(item != null && string.IsNullOrEmpty(item.TableColumnName))
					return null;
				return new
				{
					key = item != null ? item : new ClassPropertyInfo()
					{
						ClassPropertyName = p.Key,
						TableColumnName = p.Key
					},
					value = p.Value
				};
			}).Where(p => p != null).ToDictionary(p => p.key, p => p.value);
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
					list.AddRange(GetConditionName(item));
				}

				return list.ToArray();
			}
		}

		private void EachCondition(ICondition condition, out int joinStartIndex, out object[] values)
		{
			joinStartIndex = 0;
			values = new object[0];

			if(condition == null)
				return;

			if(condition is Condition)
			{
				var where = (Condition)condition;
				if(where.Value is ObjectAccessResult)
				{
					var result = (ObjectAccessResult)where.Value;
					joinStartIndex = result.JoinStartIndex;
					values = result.Values;
				}
			}
			else
			{
				var tempValues = new List<object>();

				foreach(var item in (ConditionCollection)condition)
				{
					int index;
					object[] vs;
					EachCondition(item, out index, out vs);
					joinStartIndex += index;
					tempValues.AddRange(vs);
				}

				values = tempValues.ToArray();
			}
		}

		private string ParseJoinSql(string[] includes, ClassInfo info, string tableEx, out Dictionary<string, Tuple<ClassInfo, string>> includeMapping)
		{
			var cache = new Dictionary<string, Tuple<ClassInfo, string>>();
			var result = string.Join(" ", includes.Select(p => ParseJoinSql(p, info, tableEx, cache)).Where(p => !string.IsNullOrEmpty(p)));
			includeMapping = cache;
			return result;
		}

		private string ParseJoinSql(string include, ClassInfo info, string tableEx, Dictionary<string, Tuple<ClassInfo, string>> cache)
		{
			var index = include.LastIndexOf('.');

			if(cache.ContainsKey(include))
				return null;

			ClassPropertyInfo[] pis;
			Tuple<ClassInfo, string> value;
			var joinformat = _caseSensitive ? "{0} JOIN \"{1}\" {2} ON {3}" : "{0} JOIN {1} {2} ON {3}";
			var onformat = _caseSensitive ? "{0}.\"{1}\"={2}.\"{3}\"" : "{0}.{1}={2}.{3}";

			if(index < 0)
			{
				pis = info.PropertyInfoList.Where(p => p.IsFKColumn && p.SetClassPropertyName.Equals(include, StringComparison.OrdinalIgnoreCase)).ToArray();
				value = new Tuple<ClassInfo, string>(pis[0].Join, "J" + cache.Count);
				cache.Add(include, value);

				return string.Format(joinformat, pis[0].Nullable ? "LEFT" : "INNER", value.Item1.TableName, value.Item2,
					string.Join(" AND ", pis.Select(p => string.Format(onformat, tableEx, p.TableColumnName, value.Item2, p.JoinColumn.TableColumnName))));
			}
			var key = include.Substring(0, index);
			string result = "";
			if(!cache.ContainsKey(key))
				result = ParseJoinSql(key, info, tableEx, cache);

			var tuple = cache[key];
			var property = include.Substring(index + 1);
			pis = tuple.Item1.PropertyInfoList.Where(p => p.IsFKColumn && p.SetClassPropertyName.Equals(property, StringComparison.OrdinalIgnoreCase)).ToArray();
			value = new Tuple<ClassInfo, string>(pis[0].Join, "J" + cache.Count);
			cache.Add(include, value);

			result += " " + string.Format(joinformat, pis[0].Nullable ? "LEFT" : "INNER", value.Item1.TableName, value.Item2,
				string.Join(" AND ", pis.Select(p => string.Format(onformat, tuple.Item2, p.TableColumnName, value.Item2, p.JoinColumn.TableColumnName))));

			return result;
		}
		#endregion
	}
}
