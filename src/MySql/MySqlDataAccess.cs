﻿/*
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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using MySql.Data.MySqlClient;
using Zongsoft.Data;

namespace Automao.Data.MySql
{
	public class MySqlDataAccess : ObjectAccess
	{
		#region 字段
		private Dictionary<string, DbType> _dbTypes;
		#endregion

		#region 构造函数
		public MySqlDataAccess() : base(MySqlClientFactory.Instance)
		{
		}
		#endregion

		#region 属性
		private Dictionary<string, DbType> DbTypes
		{
			get
			{
				if(_dbTypes == null)
					_dbTypes = Enum.GetValues(typeof(DbType)).Cast<DbType>().ToDictionary(p => p.ToString().ToUpper(), p => p);
				return _dbTypes;
			}
		}
		#endregion

		#region 重写方法
		protected override string CreateSelectSql(CreateSelectSqlParameter parameter)
		{
			var sql = string.Format("SELECT {0} FROM {1}", string.Join(",", parameter.Columns.Select(p => p.ToSelectColumn())), parameter.Info.GetTableName());
			if(!string.IsNullOrEmpty(parameter.Join))
				sql += " " + parameter.Join;
			if(!string.IsNullOrEmpty(parameter.Where))
				sql += " " + parameter.Where;

			if(!string.IsNullOrEmpty(parameter.Orderby))
				sql += " " + parameter.Orderby;

			if(parameter.Paging != null && parameter.Paging.PageSize > 0)
			{
				if(parameter.Paging.PageIndex < 1)
					parameter.Paging.PageIndex = 1;
				sql += string.Format(" LIMIT {0},{1}", parameter.Paging.PageSize * (parameter.Paging.PageIndex - 1), parameter.Paging.PageSize);

				//limit 不能出现在IN/ALL/ANY/SOME的子查询语句里面，但像下面这样包一层就可以
				if(parameter.Subquery && (parameter.ConditionOperator == ConditionOperator.In || parameter.ConditionOperator == ConditionOperator.NotIn))
				{
					sql = string.Format("select {0} from ({1}) {2}", string.Join(",", parameter.Columns.Select(p =>
					{
						var c = p.GetColumnEx();
						if(c == "0")
							return c;
						return string.Format("{0}.{1}", parameter.Info.AsName, c);
					})), sql, parameter.Info.AsName);
				}
			}

			return sql;
		}

		protected override string CreateSelectSql(CreateGroupSelectSqlParameter parameter)
		{
			var sql = string.Format("SELECT {0} FROM {1}", string.Join(",", parameter.Columns.Select(p => p.ToSelectColumn())), parameter.Info.GetTableName());
			if(!string.IsNullOrEmpty(parameter.Join))
				sql += " " + parameter.Join;
			if(!string.IsNullOrEmpty(parameter.Where))
				sql += " " + parameter.Where;

			if(!string.IsNullOrEmpty(parameter.Group))
				sql += " " + parameter.Group;

			if(!string.IsNullOrEmpty(parameter.Having))
				sql += " " + parameter.Having;

			string newColumns = string.Empty;
			if(parameter.GroupedSelectColumns != null && parameter.GroupedSelectColumns.Count > 0)
			{
				newColumns = string.Join(",", parameter.GroupedSelectColumns.Select(p =>
				{
					if(p.ClassInfo == parameter.Info)
						return p.ToSelectColumn(parameter.NewTableNameEx, parameter.NewTableNameEx);
					else
						return p.ToSelectColumn();
				}));
				sql = string.Format("select {0} from ({1}) {2}", string.Format("{0},{1}.*", newColumns, parameter.NewTableNameEx), sql, parameter.NewTableNameEx);
				if(!string.IsNullOrEmpty(parameter.GroupedJoin))
					sql += " " + parameter.GroupedJoin;
			}

			if(!string.IsNullOrEmpty(parameter.Orderby))
			{
				if(parameter.GroupedSelectColumns == null || parameter.GroupedSelectColumns.Count == 0)
					sql += " " + parameter.Orderby;
				else
					sql += " " + parameter.Orderby.Replace(parameter.Info.AsName + ".", parameter.NewTableNameEx + ".").Replace(parameter.Info.AsName + "_", parameter.NewTableNameEx + "_");
			}

			if(parameter.Paging != null)
			{
				if(parameter.Paging.PageIndex < 1)
					parameter.Paging.PageIndex = 1;
				sql += string.Format(" LIMIT {0},{1}", parameter.Paging.PageSize * (parameter.Paging.PageIndex - 1), parameter.Paging.PageSize);

				if(parameter.Subquery && (parameter.ConditionOperator == ConditionOperator.In || parameter.ConditionOperator == ConditionOperator.NotIn))
				{
					sql = string.Format("select {0} from ({1}) {2}", string.Join(",", parameter.GroupedSelectColumns.Select(p =>
						p.ToSelectColumn(parameter.NewTableNameEx, parameter.NewTableNameEx)
						)), sql, parameter.NewTableNameEx);
				}
			}

			return sql;
		}

		internal override DbParameter CreateParameter(int index, object value, string dbType = null, string name = null, bool isOutPut = false, bool isInOutPut = false, int? size = null, bool isProcedure = false)
		{
			var paramer = new MySqlParameter();

			paramer.ParameterName = string.IsNullOrEmpty(name) ? ("p" + index) : name;
			if(!isProcedure)
				paramer.ParameterName = "@" + paramer.ParameterName;

			if(value != null)
			{
				if(value is byte[])
				{
					paramer.Value = value;
				}
				else
					paramer.Value = this.ConvertValue(value);
			}
			else if(!string.IsNullOrEmpty(dbType))
			{
				if(DbTypes.ContainsKey(dbType.ToUpper()))
					paramer.DbType = DbTypes[dbType.ToUpper()];
			}

			if(size.HasValue)
				paramer.Size = size.Value;
			if(isOutPut)
				paramer.Direction = ParameterDirection.Output;
			else if(isInOutPut)
				paramer.Direction = ParameterDirection.InputOutput;

			return paramer;
		}
		#endregion

		internal object ConvertValue(object value)
		{
			if(value == null)
				return System.DBNull.Value;

			Type type = value.GetType();

			if(type.IsEnum)
			{
				var attris = type.GetCustomAttributes(typeof(System.ComponentModel.TypeConverterAttribute), false);
				if(attris != null && attris.Length > 0)
				{
					var converter = (System.ComponentModel.TypeConverter)System.Activator.CreateInstance(Type.GetType(((System.ComponentModel.TypeConverterAttribute)attris[0]).ConverterTypeName));
					if(converter.CanConvertTo(null))
						return converter.ConvertTo(value, null);
				}

				return Convert.ChangeType(value, Enum.GetUnderlyingType(type));
			}

			if(type == typeof(Guid))
				return ((Guid)value).ToByteArray();

			if(type == typeof(Guid?))
			{
				var v = (Guid?)value;
				if(v.HasValue)
					return v.Value.ToByteArray();
				else
					return System.DBNull.Value;
			}

			if(type == typeof(bool))
				return ((bool)value) ? 1 : 0;
			if(type == typeof(bool?))
			{
				var v = (bool?)value;
				if(v.HasValue)
					return v.Value ? 1 : 0;
				else
					return System.DBNull.Value;
			}

			return value;
		}

		protected override Dictionary<string, ColumnInfo> CreateColumnInfo(IEnumerable<string> columns, ClassInfo root)
		{
			return MySql.MySqlColumnInfo.Create(columns, root);
		}

		protected override ColumnInfo CreateColumnInfo(string original)
		{
			return new MySqlColumnInfo(original);
		}

		protected override ClassInfo CreateClassInfo(string @as, Mapping.ClassNode classNode)
		{
			return new MySqlClassInfo(@as, classNode);
		}

		protected override string GetProcedureName(Mapping.ProcedureNode procedureNode)
		{
			return procedureNode.GetProcedureName(false);
		}
	}
}
