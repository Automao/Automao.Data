using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Automao.Data.Mapping;

namespace Automao.Data
{
	public class Join
	{
		private Join _parent;
		private List<Join> _children;
		private ClassInfo _host;
		private JoinType? _changeMode;
		private string _changeHostAsname;
		private List<string> _newWhere;

		public Join(Join parent, ClassInfo host)
		{
			_children = new List<Join>();
			_parent = parent;
			_host = host;
			host.Joins.Add(this);

			_newWhere = new List<string>();
		}

		public Join Parent
		{
			get
			{
				return _parent;
			}
		}

		public ClassInfo Host
		{
			get
			{
				return _host;
			}
		}

		public JoinPropertyNode JoinInfo
		{
			get;
			set;
		}

		public ClassInfo Target
		{
			get;
			set;
		}

		/// <summary>
		/// 添加额外的join on 条件
		/// </summary>
		public void AddJoinWhere(string where)
		{
			_newWhere.Add(where);
		}

		public void ChangeMode(JoinType mode)
		{
			_changeMode = mode;
		}

		public void ChangeHostAsName(string newAsName)
		{
			_changeHostAsname = newAsName;
		}

		/// <summary>
		/// 一级一级往上，获取所有的Parent
		/// </summary>
		/// <param name="list"></param>
		/// <param name="stop"></param>
		public List<Join> GetParent(Func<Join, bool> stop)
		{
			var result = new List<Join>();

			if(this.Parent != null && !stop(this.Parent))
			{
				result.Add(this.Parent);
				result.AddRange(this.Parent.GetParent(stop));
			}

			return result;
		}

		public string ToJoinSql(Func<string, ColumnInfo> createColumnInfo)
		{
			return CreatJoinSql(this, createColumnInfo);
		}

		public static string CreatJoinSql(Join join, Func<string, ColumnInfo> createColumnInfo)
		{
			return CreatJoinSql(join, join.JoinInfo.Member.ToDictionary(jc => jc.Key.Field, jc => jc.Value.Field), createColumnInfo);
		}

		public static string CreatJoinSql(Join join, Dictionary<string, string> relation, Func<string, ColumnInfo> createColumnInfo)
		{
			var isLeftJoin = join._changeMode.HasValue ? join._changeMode.Value == JoinType.Left : join.JoinInfo.Type == JoinType.Left;
			var hostAsName = string.IsNullOrEmpty(join._changeHostAsname) ? join.Host.AsName : join._changeHostAsname;

			var joinformat = "{0} JOIN {1} ON {2}";
			return string.Format(joinformat, isLeftJoin ? "LeFT" : "INNER", join.Target.GetTableName(),
				string.Join(" AND ", relation.Select(jc => createColumnInfo(jc.Key).ToColumn(hostAsName) + "=" + createColumnInfo(jc.Value).ToColumn(join.Target.AsName))
				.Concat(join._newWhere)));
		}
	}
}
