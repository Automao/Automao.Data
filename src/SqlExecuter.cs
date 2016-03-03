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
	internal class SqlExecuter : IEnlistment, IDisposable
	{
		#region 字段
		private const string _connection_ArgumentsKey = "DbConnection";
		private const string _transaction_ArgumentsKey = "DbTransaction";
		private static SqlExecuter _current = new SqlExecuter();
		private System.Data.Common.DbProviderFactory _dbProviderFactory;
		private string _connectionString;
		private System.Data.Common.DbConnection _keepConnection;
		private bool _needKeepConnection;
		#endregion

		#region 构造函数
		private SqlExecuter()
		{
		}
		#endregion

		#region 属性
		public static SqlExecuter Current
		{
			get
			{
				return _current;
			}
		}

		public DbProviderFactory DbProviderFactory
		{
			set
			{
				_dbProviderFactory = value;
			}
		}

		public string ConnectionString
		{
			set
			{
				_connectionString = value;
			}
		}

		private DbConnection DbConnection
		{
			get
			{
				var transaction = Transaction.Current;
				if(transaction != null)
				{
					var connection = transaction.Information.Parameters.ContainsKey(_connection_ArgumentsKey)
						? transaction.Information.Parameters[_connection_ArgumentsKey] as DbConnection
						: null;

					if(connection == null)
					{
						//创建一个新的数据连接对象
						connection = _dbProviderFactory.CreateConnection();
						connection.ConnectionString = _connectionString;

						var isolationLevel = Parse(transaction.IsolationLevel);
						connection.Open();
						var dbTransaction = connection.BeginTransaction(isolationLevel);

						//设置当前事务的环境参数
						transaction.Information.Parameters[_connection_ArgumentsKey] = connection;
						transaction.Information.Parameters[_transaction_ArgumentsKey] = dbTransaction;

						transaction.Enlist(this);
					}
					return connection;
				}

				if(_needKeepConnection)
				{
					if(_keepConnection == null)
					{
						_keepConnection = _dbProviderFactory.CreateConnection();
						_keepConnection.ConnectionString = _connectionString;
					}
					return _keepConnection;
				}
				else
				{
					var connection = _dbProviderFactory.CreateConnection();
					connection.ConnectionString = _connectionString;
					return connection;
				}
			}
		}

		private DbTransaction DbTransaction
		{
			get
			{
				var transaction = Transaction.Current;
				if(transaction != null)
					return transaction.Information.Parameters[_transaction_ArgumentsKey] as DbTransaction;
				return null;
			}
		}
		#endregion

		#region 公共方法
		public SqlExecuter Keep()
		{
			var executer = new SqlExecuter();
			executer._needKeepConnection = true;
			executer._dbProviderFactory = _dbProviderFactory;
			executer._connectionString = _connectionString;
			return executer;
		}

		public void Dispose()
		{
			if(_keepConnection != null)
				_keepConnection.Dispose();
		}
		#endregion

		#region Execute
		/// <summary>
		/// 执行查找操作
		/// </summary>
		/// <param name="sql">复合格式查询语句</param>
		/// <param name="paramers"></param>
		/// <returns></returns>
		public IEnumerable<Dictionary<string, object>> Select(string sql, DbParameter[] parameters)
		{
			if(string.IsNullOrEmpty(sql))
				throw new ArgumentNullException("formatSql");

			var connection = DbConnection;

			using(var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(sql, parameters.Select(p => (object)p.ParameterName).ToArray());
				command.Parameters.AddRange(parameters);

				var transaction = DbTransaction;
				var startTransaction = transaction != null;
				if(startTransaction && !Transaction.Current.IsCompleted)
					command.Transaction = transaction;

				if(connection.State == ConnectionState.Broken)
					throw new System.Data.DataException("connection state is broken");

				try
				{
					if(connection.State == ConnectionState.Closed)
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
					if(!startTransaction && !_needKeepConnection)
						connection.Close();
				}
			}
		}

		/// <summary>
		/// 执行查询，并返回查询所返回的结果集中第一行的第一列。所有其他的列和行将被忽略。
		/// </summary>
		/// <param name="sql">复合格式查询语句</param>
		/// <returns></returns>
		public object ExecuteScalar(string sql, DbParameter[] parameters)
		{
			if(string.IsNullOrEmpty(sql))
				throw new ArgumentNullException("formatSql");

			var connection = DbConnection;

			using(var command = connection.CreateCommand())
			{
				if(parameters != null && parameters.Length > 0)
				{
					command.CommandText = string.Format(sql, parameters.Select(p => (object)p.ParameterName).ToArray());
					command.Parameters.AddRange(parameters);
				}

				var transaction = DbTransaction;
				var startTransaction = transaction != null;
				if(startTransaction && !Transaction.Current.IsCompleted)
					command.Transaction = transaction;

				if(connection.State == ConnectionState.Broken)
					throw new System.Data.DataException("connection state is broken");

				try
				{
					if(connection.State == ConnectionState.Closed)
						connection.Open();
#if DEBUG
					var result = command.ExecuteScalar();
					return result;
#else
					return command.ExecuteScalar();
#endif
				}
				finally
				{
					if(!startTransaction && !_needKeepConnection)
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
		public int Execute(string sql, DbParameter[] parameters)
		{
			if(string.IsNullOrEmpty(sql))
				throw new ArgumentNullException("formatSql");

			if(Transaction.Current != null && Transaction.Current.IsCompleted)
				return -1;

			var connection = DbConnection;

			using(var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(sql, parameters.Select(p => (object)p.ParameterName).ToArray());
				command.Parameters.AddRange(parameters);

				var transaction = DbTransaction;
				var startTransaction = transaction != null;
				if(startTransaction)
					command.Transaction = transaction;

				if(connection.State == ConnectionState.Broken)
					throw new System.Data.DataException("connection state is broken");

				try
				{
					if(connection.State == ConnectionState.Closed)
						connection.Open();
#if DEBUG
					var i = command.ExecuteNonQuery();
					return i;
#else
					return command.ExecuteNonQuery();
#endif
				}
				finally
				{
					if(!startTransaction && !_needKeepConnection)
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
		public IEnumerable<Dictionary<string, object>> ExecuteProcedure(string procedureName, DbParameter[] parameters, out Dictionary<string, object> outParamers)
		{
			if(string.IsNullOrEmpty(procedureName))
				throw new ArgumentNullException("procedureName");

			var table = new DataTable();

			var connection = _dbProviderFactory.CreateConnection();
			connection.ConnectionString = _connectionString;

			using(var command = connection.CreateCommand())
			{
				command.CommandText = procedureName;
				command.CommandType = CommandType.StoredProcedure;

				command.Parameters.AddRange(parameters);

				using(var adapter = _dbProviderFactory.CreateDataAdapter())
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
		#endregion

		#region IEnlistment成员
		public void OnEnlist(EnlistmentContext context)
		{
			var transaction = DbTransaction;
			if(transaction == null)
				return;

			var connection = DbConnection;
			switch(context.Phase)
			{
				case EnlistmentPhase.Commit:
					transaction.Commit();
					if(connection != null)
					{
						connection.Close();
						connection.Dispose();
					}
					break;
				case EnlistmentPhase.Prepare:
					break;
				case EnlistmentPhase.Abort:
				case EnlistmentPhase.Rollback:
					transaction.Rollback();
					if(connection != null)
					{
						connection.Close();
						connection.Dispose();
					}
					break;
				default:
					break;
			}
		}
		#endregion

		#region 静态方法
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
		#endregion

		private System.Data.IsolationLevel Parse(Zongsoft.Transactions.IsolationLevel level)
		{
			switch(level)
			{
				case Zongsoft.Transactions.IsolationLevel.ReadCommitted:
					return System.Data.IsolationLevel.ReadCommitted;
				case Zongsoft.Transactions.IsolationLevel.ReadUncommitted:
					return System.Data.IsolationLevel.ReadUncommitted;
				case Zongsoft.Transactions.IsolationLevel.RepeatableRead:
					return System.Data.IsolationLevel.RepeatableRead;
				case Zongsoft.Transactions.IsolationLevel.Serializable:
					return System.Data.IsolationLevel.Serializable;
				default:
					return System.Data.IsolationLevel.ReadCommitted;
			}
		}
	}
}
