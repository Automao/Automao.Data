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
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Zongsoft.Transactions;
using Automao.Data.Options.Configuration;

namespace Automao.Data
{
	internal class SqlExecuter
	{
		#region 字段
		private bool _writeSql;
		private Data.DbHelper _dbHelper;
		private DataOptionElement _option;
		private IEnlistment _enlistment;
		#endregion

		public SqlExecuter(IEnlistment enlistment, DataOptionElement option)
		{
			_option = option;
			_enlistment = enlistment;
		}

		#region 属性
		public bool WriteSql
		{
			set
			{
				_writeSql = value;
			}
		}

		internal DbHelper DbHelper
		{
			get
			{
				if(_dbHelper == null)
					_dbHelper = DbHelper.GetDBHelper(_option);
				return _dbHelper;
			}
		}

		protected DbConnection DbConnection
		{
			get
			{
				var transaction = Transaction.Current;
				if(transaction != null)
				{
					var connection = transaction.Information.Arguments["DbConnection"] as DbConnection;
					if(connection == null)
					{
						//创建一个新的数据连接对象
						connection = DbHelper.DbConnection;
						//打开当前数据连接
						connection.Open();
						//设置当前事务的环境参数
						transaction.Information.Arguments["DbConnection"] = connection;
						transaction.Information.Arguments["DbTransaction"] = connection.BeginTransaction();

						transaction.Enlist(_enlistment);
					}
					return connection;
				}

				return DbHelper.DbConnection;
			}
		}

		private DbTransaction DbTransaction
		{
			get
			{
				var transaction = Transaction.Current;
				if(transaction != null)
					return transaction.Information.Arguments["DbTransaction"] as DbTransaction;
				return null;
			}
		}
		#endregion

		/// <summary>
		/// 执行查找操作
		/// </summary>
		/// <param name="sql">复合格式查询语句</param>
		/// <param name="paramers"></param>
		/// <returns></returns>
		public IEnumerable<Dictionary<string, object>> Select(string sql, Action<DbCommand> setParameter)
		{
			if(string.IsNullOrEmpty(sql))
				throw new ArgumentNullException("formatSql");

			var connection = DbConnection;
			var falg = Transaction.Current == null;

			using(var command = connection.CreateCommand())
			{
				command.CommandText = sql;
				setParameter(command);

				WriteSQL(sql, command);

				try
				{
					if(falg)
						connection.Open();

					using(var reader = command.ExecuteReader())
					{
						while(reader.Read())
						{
							var dic = new Dictionary<string, object>();
							for(int i = 0; i < reader.FieldCount; i++)
							{
								dic.Add(reader.GetName(i), reader[i]);
							}
							yield return dic;
						}
					}
				}
				finally
				{
					if(falg)
						connection.Close();
				}
			}
		}

		/// <summary>
		/// 执行查询，并返回查询所返回的结果集中第一行的第一列。所有其他的列和行将被忽略。
		/// </summary>
		/// <param name="sql">复合格式查询语句</param>
		/// <returns></returns>
		public object ExecuteScalar(string sql, Action<DbCommand> setParameter)
		{
			if(string.IsNullOrEmpty(sql))
				throw new ArgumentNullException("formatSql");

			var connection = DbConnection;
			var falg = Transaction.Current == null;

			using(var command = connection.CreateCommand())
			{
				command.CommandText = sql;
				setParameter(command);

				try
				{
					if(falg)
						connection.Open();

					return command.ExecuteScalar();
				}
				finally
				{
					if(falg)
						connection.Close();
				}
			}
		}

		/// <summary>
		/// 执行增删改操作
		/// </summary>
		/// <param name="sql">复合格式查询语句</param>
		/// <param name="paramers"></param>
		/// <returns></returns>
		public int Execute(string sql, Action<DbCommand> setParameter)
		{
			if(string.IsNullOrEmpty(sql))
				throw new ArgumentNullException("formatSql");

			var connection = DbConnection;
			var flag = Transaction.Current == null;

			using(var command = connection.CreateCommand())
			{
				command.CommandText = sql;
				setParameter(command);

				if(DbTransaction != null)
					command.Transaction = DbTransaction;

				try
				{
					if(flag)
						connection.Open();

					var i = command.ExecuteNonQuery();
					return i;
				}
				finally
				{
					if(flag)
						connection.Close();
				}
			}
		}

		/// <summary>
		/// 执行存储过程
		/// </summary>
		/// <param name="procedureName">存储过程名</param>
		/// <param name="paramers">输入参数</param>
		/// <param name="outParamers">输出参数</param>
		/// <returns></returns>
		public IEnumerable<Dictionary<string, object>> ExecuteProcedure(string procedureName, Action<DbCommand> setParameter, out Dictionary<string, object> outParamers)
		{
			if(string.IsNullOrEmpty(procedureName))
				throw new ArgumentNullException("procedureName");

			var table = new DataTable();

			var connection = DbConnection;

			using(var command = connection.CreateCommand())
			{
				command.CommandText = procedureName;
				command.CommandType = CommandType.StoredProcedure;
				setParameter(command);

				using(var adapter = DbHelper.DbDataAdapter)
				{
					adapter.SelectCommand = command;
					adapter.Fill(table);
				}

				outParamers = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
				foreach(DbParameter item in command.Parameters)
				{
					if(item.Direction == ParameterDirection.Output || item.Direction == ParameterDirection.InputOutput)
						outParamers.Add(item.ParameterName, item.Value);
				}
			}

			var columns = table.Columns.Cast<DataColumn>().ToList();

			return table.Rows.Cast<DataRow>().Select(r => columns.ToDictionary(c => c.ColumnName, c => r[c])).ToArray();
		}

		public static object ConvertValue(object value)
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

		public void WriteSQL(string sql, DbCommand command)
		{
			if(!_writeSql)
				return;
			try
			{
				var paramersStr = command.Parameters == null ? "" :
					string.Join(Environment.NewLine, command.Parameters.Cast<DbParameter>().Select(p => string.Format("Name={0},Size={1},Value={2},DbType={3},IsInoutPut={4},IsOutPut={5}", p.ParameterName, p.Size, p.Value, p.DbType, p.Direction == ParameterDirection.InputOutput, p.Direction == ParameterDirection.Output)));

				var str = string.Format("{0}/////////////////{0}{1}{0}/////////////////{0}{2}{0}{3}{0}", Environment.NewLine, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), sql, paramersStr);
				var path = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Automao.Data.sql.txt");
				System.IO.File.AppendAllText(path, str, Encoding.UTF8);

				var fi = new System.IO.FileInfo(path);
				if(fi.Length > 1024 * 1024)
				{
					var oldPath = path.Insert(path.LastIndexOf('.'), "(old)");
					if(System.IO.File.Exists(oldPath))
						System.IO.File.Delete(oldPath);
					fi.MoveTo(oldPath);
				}
			}
			catch
			{
			}
		}

		/// <summary>
		/// 参数
		/// </summary>
		public class Parameter
		{
			public Parameter(object value, string dbType = null, string name = null, bool isOutPut = false, bool isInOutPut = false, int? size = null)
			{
				Value = value;
				Name = name;
				DbType = dbType;
				IsOutPut = isOutPut;
				IsInoutPut = isInOutPut;
				Size = size;
			}
			/// <summary>
			/// 名称
			/// </summary>
			public string Name
			{
				get;
				set;
			}
			/// <summary>
			/// 类型
			/// </summary>
			public string DbType
			{
				get;
				set;
			}
			/// <summary>
			/// 是否是输出参数
			/// </summary>
			public bool IsOutPut
			{
				get;
				set;
			}
			/// <summary>
			/// 是否是输入输出参数
			/// </summary>
			public bool IsInoutPut
			{
				get;
				set;
			}
			/// <summary>
			/// 值
			/// </summary>
			public object Value
			{
				get;
				set;
			}
			/// <summary>
			/// 大小
			/// </summary>
			public int? Size
			{
				get;
				set;
			}
		}
	}
}
