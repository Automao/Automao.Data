using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Zongsoft.Data;

namespace Automao.Data
{
	#region 委托
	public delegate CreateSqlResult CreateSql(CreatingSqlParameter parameter);
	public delegate IEnumerable<T> Execute<T>(CreateSqlResult result);
	#endregion

	public class ObjectAccessResult
	{
		#region 字段
		private CreateSql _createSql;
		#endregion

		#region 构造函数
		public ObjectAccessResult(CreateSql createSql)
		{
			_createSql = createSql;
		}
		#endregion

		#region 方法
		public CreateSqlResult CreateSql(CreatingSqlParameter parameter)
		{
			return _createSql(parameter);
		}
		#endregion
	}

	public class ObjectAccessResult<T> : ObjectAccessResult, IEnumerable<T>, IEnumerator<T>
	{
		#region 静态字段
		private static readonly object _lock = new object();
		#endregion

		#region 字段
		private IEnumerator _enumerator;
		private Execute<T> _getResult;
		#endregion

		#region 构造函数
		public ObjectAccessResult(CreateSql createSql, Execute<T> execute)
			: base(createSql)
		{
			_getResult = execute;
		}
		#endregion

		#region 属性
		public IEnumerator Enumerator
		{
			get
			{
				if(_enumerator == null)
				{
					lock(_lock)
					{
						if(_enumerator == null)
						{
							var parameter = new CreatingSqlParameter(false, 0, 0, 0);
							var result = base.CreateSql(parameter);
							_enumerator = _getResult(result).ToList().GetEnumerator();
						}
					}
				}
				return _enumerator;
			}
		}
		#endregion

		#region IEnumberable<T>成员
		public IEnumerator<T> GetEnumerator()
		{
			if(_enumerator != null)
				_enumerator.Reset();

			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			if(_enumerator != null)
				_enumerator.Reset();

			return this;
		}
		#endregion

		#region IEnumerator<T> 成员
		public void Dispose()
		{
		}

		object IEnumerator.Current
		{
			get
			{
				return _enumerator.Current;
			}
		}

		public bool MoveNext()
		{
			var enumerator = _enumerator ?? this.Enumerator;
			return enumerator.MoveNext();
		}

		public void Reset()
		{
			_enumerator.Reset();
		}

		T IEnumerator<T>.Current
		{
			get
			{
				return (T)((IEnumerator)this).Current;
			}
		}
		#endregion
	}
}
