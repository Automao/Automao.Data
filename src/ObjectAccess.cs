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
using Automao.Data.Mapping;

namespace Automao.Data
{
	public abstract class ObjectAccess : DataAccessBase
	{
		#region 字段
		private SqlExecuter _executer;
		private bool _caseSensitive;
		private DbProviderFactory _providerFactory;
		private MappingInfo _mappingInfo;
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

		protected MappingInfo MappingInfo
		{
			get
			{
				if(_mappingInfo == null)
					System.Threading.Interlocked.CompareExchange(ref _mappingInfo,
						MappingInfo.Create(Option.Mappings.Select(p => ((Options.Configuration.Mapping)p).Path).ToArray(), Option.MappingFileName), null);
				return _mappingInfo;
			}
		}

		internal SqlExecuter Executer
		{
			get
			{
				if(_executer == null)
				{
					_executer = SqlExecuter.Current;
					_executer.DbProviderFactory = _providerFactory;
					_executer.ConnectionString = Option.ConnectionString;
				}
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

			var classNode = MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(classNode == null)
				throw new Exception(string.Format("未找到{0}对应的mapping节点", name));

			classNode.SetEntityType(typeof(T));
			var classInfo = new ClassInfo("T", classNode);

			var allColumnInfos = new Dictionary<string, ColumnInfo>();

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
			allColumnInfos = ColumnInfo.Create(allColumns, classInfo);

			return new ObjectAccessResult<T>((ref int tableIndex, ref int joinStartIndex, ref int valueIndex, out object[] values) =>
			{
				classInfo.SetIndex(tableIndex++);
				joinStartIndex = classInfo.SetJoinIndex(joinStartIndex);

				var sql = CreateSelectSql(classInfo, condition, members, conditionNames, allColumnInfos, paging, grouping, sorting, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
				return sql;
			}, (sql, values) =>
			{
				var tablevalues = this.Executer.Select(sql, this.CreateParameters(0, values));
				var result = this.SetEntityValue<T>(tablevalues, classInfo);
				return result;
			});
		}

		internal string CreateSelectSql(ClassInfo classInfo, ICondition condition, string[] members, string[] conditionNames, Dictionary<string, ColumnInfo> allColumnInfos, Paging paging, Grouping grouping, Sorting[] sorting, ref int tableIndex, ref int joinStartIndex, ref int valueIndex, out object[] values)
		{
			Func<ColumnInfo, bool> predicate = (p =>
			{
				var columnInfo = p;
				if(columnInfo.PropertyNode != null)
					return !columnInfo.PropertyNode.UnColumn;//排除列名为空的字段
				return !columnInfo.ClassInfo.ClassNode.JoinList.Any(pp => pp.Name == columnInfo.Field);//排除导航属性
			});

			#region 要查询的列
			var selectMembers = members.Where(p => grouping == null || p.Contains('(') && !grouping.Members.Contains(p));
			if(grouping != null)
				selectMembers = selectMembers.Concat(grouping.Members);

			var columns = string.Join(",", selectMembers.Select(p => allColumnInfos[p]).Where(predicate).Select(p => p.ToSelectColumn(_caseSensitive)));
			#endregion

			#region where
			var where = condition.ToWhere(allColumnInfos, _caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
			#endregion

			#region join
			var tempJoinInfos = new List<Join>();
			var tempColumns = conditionNames.Concat(selectMembers);
			if(grouping != null && grouping.Condition != null)
				tempColumns = tempColumns.Concat(GetConditionName(grouping.Condition));
			foreach(var item in tempColumns.ToArray())
			{
				var columnInfo = allColumnInfos[item];
				if(columnInfo.Join != null && !tempJoinInfos.Contains(columnInfo.Join))
				{
					tempJoinInfos.Add(columnInfo.Join);
					tempJoinInfos.AddRange(columnInfo.Join.GetParent(p => tempJoinInfos.Contains(p)));
				}
			}

			var join = string.Join(" ", tempJoinInfos.OrderBy(p => p.Target.AsIndex).Select(p => p.ToJoinSql(_caseSensitive)));
			#endregion

			#region grouping
			string having = string.Empty;
			string group = string.Empty;
			string groupedSelectColumns = string.Empty;
			string groupedJoin = string.Empty;
			var newTableNameEx = classInfo.As + (classInfo.AsIndex + 1);
			if(grouping != null)
			{
				var groupColumnInfos = grouping.Members.Select(p => allColumnInfos[p]).ToArray();
				group = string.Format("GROUP BY {0}", string.Join(",", groupColumnInfos.Select(p => p.ToColumn(_caseSensitive))));

				var groupedSelectMembers = members.Where(p => !p.Contains('(') && !grouping.Members.Contains(p));
				groupedSelectColumns = string.Join(",", groupedSelectMembers.Select(p => allColumnInfos[p]).Where(predicate).Select(p => p.ToSelectColumn(_caseSensitive)));

				tempJoinInfos = new List<Join>();
				foreach(var item in groupedSelectMembers)
				{
					var columnInfo = allColumnInfos[item];
					if(columnInfo.Join != null && !tempJoinInfos.Contains(columnInfo.Join))
					{
						tempJoinInfos.Add(columnInfo.Join);
						tempJoinInfos.AddRange(columnInfo.Join.GetParent(p => groupColumnInfos.Any(pp => pp.Join == p) || tempJoinInfos.Contains(p)));
					}
				}

				groupedJoin = string.Join(" ", tempJoinInfos.OrderBy(p => p.Target.AsIndex).Select(p =>
				{
					var groupColumnInfo = groupColumnInfos.Where(gc => gc.Join == p.Parent).ToArray();
					if(groupColumnInfo.Any())
					{
						var dic = p.JoinInfo.Member.ToDictionary(c => groupColumnInfo.FirstOrDefault(gc => gc.Field == c.Key.Name).GetColumnEx(_caseSensitive),
							c => c.Key.Column);

						if(p.JoinInfo.Type == JoinType.Left)
						{
							var joinInfo = new JoinPropertyNode(p.JoinInfo.Name, p.JoinInfo.Target);
							joinInfo.Member = p.JoinInfo.Member;
							p.JoinInfo = joinInfo;
						}

						return Join.CreatJoinSql(_caseSensitive, p, newTableNameEx, dic);
					}
					return p.ToJoinSql(_caseSensitive);
				}));

				if(grouping.Condition != null)
				{
					object[] tempValues;
					having = grouping.Condition.ToWhere(allColumnInfos, _caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out tempValues, "HAVING {0}");
					values = values.Concat(tempValues).ToArray();
				}
			}
			#endregion

			var orderby = sorting == null || sorting.Length == 0 ? "" : string.Format("ORDER BY {0}", string.Join(",", sorting.Select(p => p.Parse(allColumnInfos, _caseSensitive))));

			string sql;
			if(string.IsNullOrEmpty(group))
				sql = CreateSelectSql(classInfo, columns, where, join, orderby, paging);
			else
				sql = CreateSelectSql(classInfo, newTableNameEx, columns, where, join,
					group, having, groupedSelectColumns, groupedJoin, orderby, paging);

			if(paging != null && paging.TotalCount == 0)
			{
				var countSql = string.Empty;
				if(string.IsNullOrEmpty(group))
					countSql = CreateSelectSql(classInfo, "COUNT(0)", where, join, null, null);
				else
					countSql = CreateSelectSql(classInfo, newTableNameEx, columns, where, join,
						group, having, "COUNT(0)", groupedJoin, null, null);

				var tablevalues = this.Executer.Select(countSql, CreateParameters(0, values));

				if(tablevalues != null)
				{
					var item = tablevalues.FirstOrDefault();
					paging.TotalCount = Convert.ToInt32(item.Values.First());
				}
			}

			return sql;
		}

		protected abstract string CreateSelectSql(ClassInfo info, string columns, string where, string join, string orderby, Paging paging);

		protected abstract string CreateSelectSql(ClassInfo info, string newTableNameEx, string columns, string where, string join, string group, string having, string groupedSelectColumns, string groupedJoin, string orderby, Paging paging);
		#endregion

		#region Count
		protected override int Count(string name, ICondition condition, string[] includes)
		{
			if(string.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			var info = this.MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var joinInfos = new Dictionary<string, Join>();
			var allColumnInfos = new Dictionary<string, ColumnInfo>();

			var conditionNames = GetConditionName(condition);
			var allcolumns = conditionNames.Concat(includes);

			var classInfo = new ClassInfo("T", info);
			allColumnInfos = ColumnInfo.Create(allcolumns, classInfo);
			classInfo.SetJoinIndex(0);

			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			object[] values;
			var whereSql = condition.ToWhere(allColumnInfos, _caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);

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
			var joinsql = string.Join(" ", tempJoinInfos.OrderBy(p => p.Target.AsIndex).Select(p => p.ToJoinSql(_caseSensitive)));

			string countSql;
			if(includes == null || includes.Length == 0)
				countSql = "COUNT(0)";
			else if(includes.Length == 1)
				countSql = string.Format("COUNT({0})", allColumnInfos[includes[0]].ToColumn(_caseSensitive));
			else
				countSql = string.Format("COUNT(CONCAT({0}))", string.Join(",", includes.Select(p => allColumnInfos[p]).Select(p => p.ToColumn(_caseSensitive))));

			var sql = string.Format("SELECT {0} FROM {1} T {2} {3}", countSql, info.GetTableName(_caseSensitive), joinsql, whereSql);

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

			var info = this.MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var conditionNames = GetConditionName(condition);
			var classInfo = new ClassInfo("T", info);
			var columnInfos = ColumnInfo.Create(conditionNames, classInfo);
			classInfo.SetJoinIndex(0);

			var values = new object[0];
			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			var whereSql = condition.ToWhere(columnInfos, _caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);

			var sql = string.Format("DELETE FROM {0} {1}", info.GetTableName(_caseSensitive), whereSql);
			return Executer.Execute(sql, CreateParameters(0, values));
		}
		#endregion

		#region 执行
		public override object Execute(string name, IDictionary<string, object> inParameters, out IDictionary<string, object> outParameters)
		{
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

			var procedureName = procedureInfo.GetProcedureName(_caseSensitive);
			var tablevalues = Executer.ExecuteProcedure(procedureName, paramers.ToArray(), out dic);

			outParameters = dic.ToDictionary(p => p.Key, p => p.Value);

			if(classInfo != null)
				return tablevalues.Select(p => CreateEntity<object>(classInfo.EntityType, p, classInfo));
			else
			{
				var item = tablevalues.FirstOrDefault();
				if(item == null || item.Count == 0)
					return item;

				return item[item.Keys.FirstOrDefault()];
			}
		}
		#endregion

		#region 新增
		protected override int Insert<T>(string name, IEnumerable<T> entities, string[] includes)
		{
			if(!entities.GetEnumerator().MoveNext())
				return 0;

			if(string.IsNullOrEmpty(name))
				name = typeof(T).Name;

			var info = MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var insertCount = 0;
			var sqls = new List<KeyValuePair<string, DbParameter[]>>();

			var insertformat = "INSERT INTO {0}({1}) VALUES({2})";
			var columnformat = _caseSensitive ? "\"{0}\"" : "{0}";
			var tableName = info.GetTableName(_caseSensitive);
			foreach(var item in entities)
			{
				Dictionary<PropertyNode, object> pks;
				var dic = GetColumnFromEntity(info, item, null, out pks).Where(p => p.Value != null && includes.Contains(p.Key.Name, StringComparer.OrdinalIgnoreCase)).ToDictionary(p => p.Key, p => p.Value);

				var sql = string.Format(insertformat, tableName,
					string.Join(",", dic.Keys.Select(p => string.Format(columnformat, p.Column))),
					string.Join(",", dic.Select((p, i) => string.Format("{{{0}}}", i)))
				);

				var paramers = dic.Select((p, i) => CreateParameter(i, p.Value)).ToArray();

				sqls.Add(new KeyValuePair<string, DbParameter[]>(sql, paramers));
			}

			using(var executer = Executer.Keep())
			{
				foreach(var item in sqls)
				{
					var count = executer.Execute(item.Key, item.Value);
					if(count > 0)
						insertCount++;
				}
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

			var info = MappingInfo.ClassNodeList.FirstOrDefault(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
			if(info == null)
				throw new Exception(string.Join("未找到{0}对应的Mapping", name));

			var sqls = new List<KeyValuePair<string, DbParameter[]>>();
			var classInfo = new ClassInfo("T", info);

			var columnformat = _caseSensitive ? "\"{0}\"={{{1}}}" : "{0}={{{1}}}";
			var nullcolumnformat = _caseSensitive ? "\"{0}\"=NULL" : "{0}=NULL";
			var updateformat = "UPDATE {0} T SET {1} {2}";
			var tableName = info.GetTableName(_caseSensitive);

			var values = new object[0];
			int tableIndex = 0;
			int joinStartIndex = 0;
			int valueIndex = 0;
			string wheresql = string.Empty;
			if(condition != null)
			{
				var columns = GetConditionName(condition);
				var columnInofs = ColumnInfo.Create(columns, classInfo);
				classInfo.SetJoinIndex(0);
				wheresql = condition.ToWhere(columnInofs, _caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
			}

			foreach(var entity in entities)
			{
				Dictionary<PropertyNode, object> pks;
				var dic = GetColumnFromEntity(info, entity, members, out pks);

				if(condition == null)//condition为空则跟据主键修改
				{
					if(pks == null || pks.Count == 0)
						throw new ArgumentException("未设置Condition，也未找到主键");

					var newCondition = new ConditionCollection(ConditionCombine.And, pks.Select(p => new Condition(p.Key.Name, p.Value)));

					classInfo.Joins.Clear();
					var columnInfos = ColumnInfo.Create(pks.Select(p => p.Key.Name), classInfo);
					classInfo.SetJoinIndex(0);

					wheresql = newCondition.ToWhere(columnInfos, _caseSensitive, ref tableIndex, ref joinStartIndex, ref valueIndex, out values);
				}

				var temp = dic.Where(p => p.Value != null);
				var list = temp.Select((p, i) => string.Format(columnformat, p.Key.Column, i)).ToList();
				var paramers = temp.Select((p, i) => CreateParameter(i, p.Value)).ToList();

				list.AddRange(dic.Where(p => p.Value == null).Select(p => string.Format(nullcolumnformat, p.Key.Column)));
				paramers.AddRange(CreateParameters(paramers.Count, values));

				var sql = string.Format(updateformat, tableName, string.Join(",", list), wheresql);

				sqls.Add(new KeyValuePair<string, DbParameter[]>(sql, paramers.ToArray()));
			}

			var updateCount = 0;

			using(var executer = Executer.Keep())
			{
				foreach(var sql in sqls)
				{
					updateCount += executer.Execute(sql.Key, sql.Value);
				}
			}

			return updateCount;
		}
		#endregion

		protected override Type GetEntityType(string name)
		{
			return MappingInfo.ClassNodeList.Where(p => p.Name.Equals(name, StringComparison.OrdinalIgnoreCase)).Select(p => p.EntityType).FirstOrDefault();
		}
		#endregion

		#region 方法
		internal IEnumerable<T> SetEntityValue<T>(IEnumerable<Dictionary<string, object>> table, ClassInfo classInfo)
		{
			foreach(var row in table)
			{
				var values = row.Where(p => p.Key.StartsWith(classInfo.AsName + "_")).ToDictionary(p => p.Key.Substring(classInfo.AsName.Length + 1), p => p.Value);
				var entityType = classInfo.ClassNode.EntityType;
				var entity = CreateEntity<T>(entityType, values, classInfo.ClassNode);

				SetNavigationProperty(classInfo, entity, row);

				yield return entity;
			}
		}

		private void SetNavigationProperty(ClassInfo classInfo, object entity, Dictionary<string, object> values)
		{
			if(classInfo.Joins != null)
			{
				var type = entity.GetType();
				foreach(var item in classInfo.Joins)
				{
					var dic = values.Where(p => p.Key.StartsWith(item.Target.AsName + "_")).ToDictionary(p => p.Key.Substring(item.Target.AsName.Length + 1), p => p.Value);
					if(dic == null || dic.Count == 0 || dic.All(p => p.Value is System.DBNull))
						continue;

					if(IsDictionary(type))
					{
						((IDictionary)entity).Add(item.JoinInfo.Name, dic);
						SetNavigationProperty(item.Target, entity, values);
						continue;
					}

					var value = CreateEntity<object>(item.Target.ClassNode.EntityType, dic, item.Target.ClassNode);

					SetNavigationProperty(item.Target, value, values);

					var property = type.GetProperty(item.JoinInfo.Name);
					property.SetValue(entity, value);
				}
			}
		}

		protected T CreateEntity<T>(Type entityType, Dictionary<string, object> propertyValues, ClassNode classNode)
		{
			if(IsDictionary(entityType))
				return (T)(object)propertyValues;

			System.Reflection.ParameterInfo[] cpinfo = null;
			object[] instanceArgs = null;
			if(classNode != null)
			{
				var constructorPropertys = classNode.PropertyNodeList.Where(p => p.PassedIntoConstructor).ToList();
				cpinfo = entityType.GetConstructors().Where(p => p.IsPublic).Select(p => p.GetParameters()).FirstOrDefault(p => p.Length == constructorPropertys.Count);
				instanceArgs = new object[constructorPropertys.Count];
				constructorPropertys.ForEach(p =>
				{
					var tempValue = propertyValues.FirstOrDefault(pp => pp.Key.Equals(p.Column, StringComparison.OrdinalIgnoreCase));
					var args = cpinfo == null ? null : cpinfo.FirstOrDefault(pp => pp.Name.Equals(p.ConstructorName, StringComparison.OrdinalIgnoreCase));
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

				var propertyInfo = classNode == null ? null : classNode.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(property.Name, StringComparison.CurrentCultureIgnoreCase));
				if(propertyInfo != null && string.IsNullOrEmpty(propertyInfo.Column))
					continue;

				var tempValue = propertyValues.FirstOrDefault(p => p.Key.Equals(propertyInfo != null ? propertyInfo.Column : property.Name, StringComparison.OrdinalIgnoreCase));
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
			IDictionary<string, object> properties;
			pks = new Dictionary<PropertyNode, object>();

			if(entity is IDictionary<string, object>)
				properties = (IDictionary<string, object>)entity;
			else
			{
				Type type = entity.GetType();
				properties = new Dictionary<string, object>();
				foreach(var property in type.GetProperties())
				{
					object value = null;

					if((property.PropertyType.IsValueType || property.PropertyType == typeof(string) || property.PropertyType == typeof(byte[])))
					{
						value = property.GetValue(entity, null);
						properties.Add(property.Name, value);
					}

					var propertyNo = classNode.PropertyNodeList.FirstOrDefault(p => p.Name.Equals(property.Name, StringComparison.OrdinalIgnoreCase));
					if(propertyNo != null)
					{
						if(value == null)
							value = property.GetValue(entity, null);
						pks.Add(propertyNo, value);
					}
				}
			}

			var list = properties.Where(p => members == null || members.Contains(p.Key, StringComparer.OrdinalIgnoreCase));

			return list.Select(p =>
			{
				var item = classNode.PropertyNodeList.FirstOrDefault(pp => pp.Name.Equals(p.Key, StringComparison.CurrentCultureIgnoreCase));
				if(item != null && item.UnColumn)
					return null;
				return new
				{
					key = item != null ? item : new PropertyNode(p.Key),
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

		private bool IsDictionary(Type type)
		{
			if(type.IsSubclassOf(typeof(Dictionary<string, object>)))
				return false;

			return type == typeof(IDictionary<string, object>)
				|| type == typeof(IDictionary)
				|| type.IsSubclassOf(typeof(IDictionary<string, object>))
				|| type.IsSubclassOf(typeof(IDictionary));
		}
		#endregion
	}
}
