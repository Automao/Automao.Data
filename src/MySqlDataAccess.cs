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
			: base(false)
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
		protected override string CreateSelectSql(string tableName, string tableNameEx, string columns, string where, string join, string orderby, Paging paging)
		{
			var sql = string.Format("SELECT {0} FROM {1} {2}", columns, tableName, tableNameEx);
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

		internal override void SetParameter(DbCommand command, SqlExecuter.Parameter[] paramers)
		{
			if(paramers != null && paramers.Length != 0)
			{
				var paramerNames = new string[paramers.Length];
				for(int i = 0; i < paramers.Length; i++)
				{
					var paramer = command.CreateParameter();
					var p = paramers[i];
					var parameterName = string.IsNullOrEmpty(p.Name) ? ("p" + i) : p.Name;
					parameterName = command.CommandType == CommandType.StoredProcedure ? parameterName : ("@" + parameterName);
					paramer.ParameterName = parameterName;
					paramerNames[i] = parameterName;

					if(p.Value != null)
					{
						if(p.Value is byte[])
						{
							paramer.Value = p.Value;
						}
						else
							paramer.Value = SqlExecuter.ConvertValue(p.Value);
					}
					else if(!string.IsNullOrEmpty(p.DbType))
					{
						if(DbTypes.ContainsKey(p.DbType.ToUpper()))
							paramer.DbType = DbTypes[p.DbType.ToUpper()];
					}

					if(p.Size.HasValue)
						paramer.Size = p.Size.Value;
					if(p.IsOutPut)
						paramer.Direction = ParameterDirection.Output;
					else if(p.IsInoutPut)
						paramer.Direction = ParameterDirection.InputOutput;

					command.Parameters.Add(paramer);
				}
				command.CommandText = string.Format(command.CommandText, paramerNames);
			}
		}
		#endregion
	}
}
