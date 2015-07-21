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
	public class ObjectAccessResult
	{
		#region 字段
		private int _joinStartIndex;
		private object[] _values;
		private string _executeSql;
		private int _index;
		#endregion

		#region 构造函数
		public ObjectAccessResult(int index, int joinStartIndex, object[] values, string executeSql)
		{
			_index = index;
			_joinStartIndex = joinStartIndex;
			_values = values;
			_executeSql = executeSql;
		}
		#endregion

		#region 属性
		public int Index
		{
			get
			{
				return _index;
			}
		}

		public int JoinStartIndex
		{
			get
			{
				return _joinStartIndex;
			}
		}

		public object[] Values
		{
			get
			{
				return _values;
			}
		}

		public string ExecuteSql
		{
			get
			{
				return _executeSql;
			}
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
		private Func<IEnumerable<T>> _getResult;
		private int _index;
		#endregion

		#region 构造函数
		public ObjectAccessResult(int index, int joinStartIndex, object[] values, string executeSql, Func<IEnumerable<T>> getResult)
			: base(index,joinStartIndex, values, executeSql)
		{
			_getResult = getResult;
			_index = index;
		}
		#endregion

		#region 属性
		public IEnumerator Enumerator
		{
			get
			{
				if(_enumerator == null)
					lock(_lock)
						if(_enumerator == null)
							_enumerator = _getResult().GetEnumerator();
				return _enumerator;
			}
		}
		#endregion

		#region IEnumberable<T>成员
		public IEnumerator<T> GetEnumerator()
		{
			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.Enumerator;
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
				return ((IEnumerable)this).GetEnumerator().Current;
			}
		}

		public bool MoveNext()
		{
			return this.Enumerator.MoveNext();
		}

		public void Reset()
		{
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
