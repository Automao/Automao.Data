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
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using Zongsoft.Data;

namespace Automao.Data
{
	public class MySqlDataAccess : ObjectAccess
	{
		#region 字段
		private Dictionary<string, DbType> _dbTypes;
		#endregion

		#region 构造函数
		public MySqlDataAccess()
			: base(false, MySql.Data.MySqlClient.MySqlClientFactory.Instance)
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
		protected override string CreateSelectSql(ClassInfo info, string columns, string where, string join, string orderby, Paging paging)
		{
			var sql = string.Format("SELECT {0} FROM {1}", columns, info.GetTableName(false));
			if(!string.IsNullOrEmpty(join))
				sql += " " + join;
			if(!string.IsNullOrEmpty(where))
				sql += " " + where;

			if(!string.IsNullOrEmpty(orderby))
				sql += " " + orderby;

			if(paging != null)
			{
				if(paging.PageIndex < 1)
					paging.PageIndex = 1;
				sql += string.Format(" LIMIT {0},{1}", paging.PageSize * (paging.PageIndex - 1), paging.PageSize);
			}

			return sql;
		}

		protected override string CreateSelectSql(ClassInfo info, string newTableNameEx, string columns, string where, string join, string group, string having, string groupedSelectColumns, string groupedJoin, string orderby, Paging paging)
		{
			var sql = string.Format("SELECT {0} FROM {1}", columns, info.GetTableName(false));
			if(!string.IsNullOrEmpty(join))
				sql += " " + join;
			if(!string.IsNullOrEmpty(where))
				sql += " " + where;

			if(!string.IsNullOrEmpty(group))
				sql += " " + group;

			if(!string.IsNullOrEmpty(having))
				sql += " " + having;

			if(!string.IsNullOrEmpty(groupedSelectColumns))
			{
				var newColumns = groupedSelectColumns.Replace(info.AsName + ".", newTableNameEx + ".").Replace(info.AsName + "_", newTableNameEx + "_");
				sql = string.Format("select {0} {1} from ({2}) {3}", newColumns.Equals("count(0)", StringComparison.OrdinalIgnoreCase) ? "" : string.Format("{0}.*,", newTableNameEx), newColumns, sql, newTableNameEx);
				if(!string.IsNullOrEmpty(groupedJoin))
					sql += " " + groupedJoin;
			}

			if(!string.IsNullOrEmpty(orderby))
			{
				if(!string.IsNullOrEmpty(groupedSelectColumns))
					sql += " " + orderby.Replace(info.AsName + ".", newTableNameEx + ".").Replace(info.AsName + "_", newTableNameEx + "_");
				else
					sql += " " + orderby;
			}

			if(paging != null)
			{
				if(paging.PageIndex < 1)
					paging.PageIndex = 1;
				sql += string.Format(" LIMIT {0},{1}", paging.PageSize * (paging.PageIndex - 1), paging.PageSize);
			}

			return sql;
		}

		internal override DbParameter CreateParameter(int index, object value, string dbType = null, string name = null, bool isOutPut = false, bool isInOutPut = false, int? size = null, bool isProcedure = false)
		{
			var paramer = new MySql.Data.MySqlClient.MySqlParameter();

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
					paramer.Value = SqlExecuter.ConvertValue(value);
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
	}
}
