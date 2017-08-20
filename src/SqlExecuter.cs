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

using Zongsoft.Transactions;

namespace Automao.Data
{
	internal class SqlExecuter : IEnlistment
	{
		#region 常量定义
		private const string SESSION_CONNECTION_KEY = "DbConnection";
		private const string SESSION_TRANSACTION_KEY = "DbTransaction";
		#endregion

		#region 成员字段
		private DbProviderFactory _dbProviderFactory;
		private string _connectionString;
		#endregion

		#region 构造函数
		private SqlExecuter(DbProviderFactory provider)
		{
			_dbProviderFactory = provider;
		}
		#endregion

		#region 静态方法
		public static SqlExecuter GetInstance(DbProviderFactory provider)
		{
			if(provider == null)
				throw new ArgumentNullException("provider");

			return new SqlExecuter(provider);
		}
		#endregion

		#region 公共属性
		public string ConnectionString
		{
			get
			{
				return _connectionString;
			}
			set
			{
				if(string.IsNullOrWhiteSpace(value))
					throw new ArgumentNullException();

				_connectionString = value;
			}
		}
		#endregion

		#region 执行方法
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

			var connection = this.GetDbConnection();

			using(var command = connection.CreateCommand())
			{
				if(parameters != null && parameters.Length > 0)
				{
					command.CommandText = string.Format(sql, parameters.Select(p => (object)p.ParameterName).ToArray());
					command.Parameters.AddRange(parameters);
				}
				else
					command.CommandText = sql;

				var transaction = this.GetDbTransaction();
				if(transaction != null)
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
					if(transaction == null)
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

			var connection = this.GetDbConnection();

			using(var command = connection.CreateCommand())
			{
				if(parameters != null && parameters.Length > 0)
				{
					command.CommandText = string.Format(sql, parameters.Select(p => (object)p.ParameterName).ToArray());
					command.Parameters.AddRange(parameters);
				}
				else
					command.CommandText = sql;

				var transaction = this.GetDbTransaction();
				if(transaction != null)
					command.Transaction = transaction;

				if(connection.State == ConnectionState.Broken)
					throw new System.Data.DataException("connection state is broken");

				try
				{
					if(connection.State == ConnectionState.Closed)
						connection.Open();

					var result = command.ExecuteScalar();
					return result;
				}
				catch(global::MySql.Data.MySqlClient.MySqlException ex)
				{
					throw ExceptionUtility.GetDataException(ex);
				}
				finally
				{
					if(transaction == null)
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

			var connection = this.GetDbConnection();

			using(var command = connection.CreateCommand())
			{
				if(parameters != null && parameters.Length > 0)
				{
					command.CommandText = string.Format(sql, parameters.Select(p => (object)p.ParameterName).ToArray());
					command.Parameters.AddRange(parameters);
				}
				else
					command.CommandText = sql;

				var transaction = this.GetDbTransaction();
				if(transaction != null)
					command.Transaction = transaction;

				if(connection.State == ConnectionState.Broken)
					throw new System.Data.DataException("connection state is broken");

				try
				{
					if(connection.State == ConnectionState.Closed)
						connection.Open();

					return command.ExecuteNonQuery();
				}
				catch(global::MySql.Data.MySqlClient.MySqlException ex)
				{
					throw ExceptionUtility.GetDataException(ex);
				}
				finally
				{
					if(transaction == null)
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
				var adapter = _dbProviderFactory.CreateDataAdapter();

				try
				{
					adapter.SelectCommand = command;
					adapter.Fill(table);
				}
				catch(global::MySql.Data.MySqlClient.MySqlException ex)
				{
					throw ExceptionUtility.GetDataException(ex);
				}
				finally
				{
					if(adapter != null)
						adapter.Dispose();
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

		#region 事务处理
		public void OnEnlist(EnlistmentContext context)
		{
			object value;

			if(context.Transaction.Information.Parameters.TryGetValue(SESSION_TRANSACTION_KEY, out value) && value != null && value is DbTransaction)
			{
				var transaction = (DbTransaction)value;
				var connection = transaction.Connection;

				switch(context.Phase)
				{
					case EnlistmentPhase.Prepare:
						break;
					case EnlistmentPhase.Commit:
						transaction.Commit();
						if(connection != null)
						{
							connection.Close();
							connection.Dispose();
						}
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
		}
		#endregion

		#region 私有方法
		private Transaction GetRootTransaction()
		{
			var current = Transaction.Current;

			if(current == null)
				return null;

			while(current.Information.Parent != null)
			{
				current = current.Information.Parent;
			}

			return current.IsCompleted ? null : current;
		}

		private DbConnection GetDbConnection()
		{
			var transaction = this.GetRootTransaction();

			if(transaction != null)
			{
				//如果注册成功，则表示当前为首次对根事务进行注册
				if(transaction.Enlist(this))
				{
					//创建一个新的数据连接对象
					var connection = this.CreateConnection();

					//将外部事物隔离级别转换成ADO.NET中的数据库事物隔离级别
					var isolationLevel = ConvertIsolationLevel(transaction.IsolationLevel);

					//打开数据连接
					connection.Open();

					//启动当前连接上的根数据事务
					var dbTransaction = connection.BeginTransaction(isolationLevel);

					//设置当前事务的环境参数
					transaction.Information.Parameters[SESSION_CONNECTION_KEY] = connection;
					transaction.Information.Parameters[SESSION_TRANSACTION_KEY] = dbTransaction;

					return connection;
				}
				else
				{
					object value;

					if(transaction.Information.Parameters.TryGetValue(SESSION_CONNECTION_KEY, out value) && value is DbConnection)
						return (DbConnection)value;

					throw new InvalidOperationException("Can not obtain DbConnection in the transaction context.");
				}
			}

			//返回一个创建的新连接
			return this.CreateConnection();
		}

		private DbTransaction GetDbTransaction()
		{
			var transaction = this.GetRootTransaction();
			object value;

			if(transaction != null && transaction.Information.Parameters.TryGetValue(SESSION_TRANSACTION_KEY, out value))
				return value as DbTransaction;

			return null;
		}

		private DbConnection CreateConnection()
		{
			var connection = _dbProviderFactory.CreateConnection();
			connection.ConnectionString = _connectionString;
			return connection;
		}

		private System.Data.IsolationLevel ConvertIsolationLevel(Zongsoft.Transactions.IsolationLevel level)
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
		#endregion
	}
}
