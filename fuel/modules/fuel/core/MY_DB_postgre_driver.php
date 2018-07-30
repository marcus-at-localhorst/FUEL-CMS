<?php  if ( ! defined('BASEPATH')) exit('No direct script access allowed');
/**
 * FUEL CMS
 * http://www.getfuelcms.com
 *
 * An open source Content Management System based on the
 * Codeigniter framework (http://codeigniter.com)
 *
 * @package		FUEL CMS
 * @author		David McReynolds @ Daylight Studio
 * @copyright	Copyright (c) 2018, Daylight Studio LLC.
 * @license		http://docs.getfuelcms.com/general/license
 * @link		http://www.getfuelcms.com
 */

// ------------------------------------------------------------------------

/**
 * Extends the MySQLi driver to add some extra goodness
 *
 * @package		FUEL CMS
 * @subpackage	Libraries
 * @category	Libraries
 * @author		David McReynolds @ Daylight Studio
 * @link		http://docs.getfuelcms.com/libraries/my_db_mysql_driver
 */

class MY_DB_postgre_driver extends CI_DB_postgre_driver {

	protected $_table_info_cache = array();

	// --------------------------------------------------------------------

	/**
	 * Echos out the last query ran to the screen
	 *
	 * @access	public
	 * @param	boolean	will hide the echoed output in a comment
	 * @param	boolean will exit the script
	 * @param	boolean returns the output
	 * @return	mixed
	 */
	public function debug_query($hidden = FALSE, $exit = FALSE, $return = FALSE)
	{

		$str = '';

		if (!empty($hidden)) echo '<!--';
		$str.= $this->last_query()." \n";
		if (!empty($hidden)) $str.= '-->';

		if (!empty($return))
		{
			return $str;
		}
		else
		{
		echo $str;
		}

		if (!empty($exit)) exit;
	}

	// --------------------------------------------------------------------

	/**
	 * Load the result drivers. Overrides the CI_DB_mysqli_driver driver
	 *
	 * @access	public
	 * @return	object
	 */
	public function load_rdriver()
	{
		$driver = 'MY_DB_postgre_result';

		if ( ! class_exists($driver))
		{
			include_once(BASEPATH.'database/DB_result'.EXT);
			include_once(APPPATH.'core/MY_DB_postgre_result'.EXT);
		}

		return $driver;
	}

	// --------------------------------------------------------------------

	/**
	 * Appends the table name to fields in a select that don't have it to prevent ambiguity
	 *
	 * @access	public
	 * @param	string
	 * @param	array
	 * @param	string
	 * @return	string
	 */
	public function safe_select($table, $fields = NULL, $prefix = NULL)
	{
		if (empty($prefix)) $prefix = $table.'.';
		if (empty($fields)) {
			$fields = $this->field_data($table);
			$new_fields = array();
			foreach($fields as $key => $val)
			{
				$new_fields[$val->name] = get_object_vars($val);
			}
			$fields = $new_fields;
		}
		$select = '';
		if (!empty($fields))
		{
			foreach($fields as $key => $val)
			{
				$select .= $table.'.'.$key.' as \''.$prefix.$key.'\', ';
			}
			$select = substr($select, 0, -2); // remove trailing comma
		}
		return $select;
	}

	// --------------------------------------------------------------------

	/**
	 * Gets an array of information about a particular table's field
	 *
	 * @access	public
	 * @param	string	name of table
	 * @param	string	field name
	 * @return	string
	 */
	public function field_info($table, $field)
	{
		$table_info = $this->table_info($table);
		if (isset($table_info[$field]))
		{
			return $table_info[$field];
		}
		return FALSE;
	}

	// --------------------------------------------------------------------

	/**
	 * Returns column name of primary key field
	 *
	 * @link    <https://archive.is/AUvdH>
	 * @param	string	$table
	 * @return	array
	 */
	public function get_primary_key($table)
	{
		$sql = "SELECT c.column_name, c.ordinal_position
			FROM information_schema.key_column_usage AS c
			LEFT JOIN information_schema.table_constraints AS t
			ON t.constraint_name = c.constraint_name
			WHERE t.table_name = '". strtolower($table) ."' AND t.constraint_type = 'PRIMARY KEY'";

		if (($query = $this->query($sql)) === FALSE)
		{
			return FALSE;
		}
		$result = $query->row_array();
		return element('column_name', $result);
	}

	/**
	 * Gets an array of information about a table. Useful for generating forms
	 * More extensive than CI $this->field_data($table)
	 * 
	 * TODO: normalize special field types.
	 *
	 * @access	public
	 * @param	string	name of table
	 * @param	string	field name
	 * @return	string
	 */
	public function table_info($table, $set_field_key = TRUE)
	{
		 // lazy load
		if (!empty($this->_table_info_cache[$table]) AND $set_field_key)
		{
			return $this->_table_info_cache[$table];
		}

		$sql = "SELECT * FROM information_schema.columns WHERE  table_name = '{$table}'";
		$query = $this->query($sql);
		$retval = array();
		$pk = $this->get_primary_key($table);

		//d($query->result());
		foreach($query->result() as $field) {

			// http://docs.getfuelcms.com/general/forms#universal_attributes
			// field mapping
			$type = $field->data_type;

			// https://en.wikibooks.org/wiki/Converting_MySQL_to_PostgreSQL
			switch($field->data_type) {

				case "character varying":
				case "character":
					$type = 'string';
				break;
				case "numeric":
					$type = 'float';
				break;
				case "integer":
					$type = 'int';
				break;

				// TODO: normalize more type if needed

			}


			$f = array(
				'name'        => $field->column_name,//string (2) "id"
				'org_type'    => $field->data_type,//string (3) "int"
				'type'        => $type,//string (3) "int"
				'org_default' => $field->column_default,//null
				'default'     => $field->column_default,//null
				'options'     => '',//null
				'max_length'  => ($field->character_maximum_length > 0) ? $field->character_maximum_length : $field->numeric_precision,//string (2) "10"
				'primary_key' => (bool) ($field->column_name == $pk) ,//boolean true
				'comment'     => '',//string (0) ""
				'collation'   => '',//null
				// TODO: not sure how to detect auto_increment in postgres. not sure if I need that.
				'extra'       => '',//string (14) "auto_increment"
				'null'        => ($field->is_nullable == "NO") ? FALSE : TRUE,//boolean false
			);

			if($f['primary_key']){
				$f['default'] = 'DEFAULT';
			}

			if ($set_field_key)
			{
				$retval[$f['name']] = $f;
			} else {
				$retval[] = $f;
			}
		}

		#ddd($retval);

		$this->_table_info_cache[$table] = $retval;
		return $retval;

	}

	// --------------------------------------------------------------------

	/**
	 * Save's information to the database using postgre INSERT ON CONFLICT syntax
	 *
	 * @access	public
	 * @param	string	name of table
	 * @param	array	values to save
	 * @param	mixed	primary key value(s)
	 * @return	string
	 */
	public function insert_ignore($table, $values, $primary_key = 'id')
	{
		if (empty($values)) return false;

		// get table/field meta data to format entries
		$meta = $this->table_info($table);

		$sql = "INSERT ";
		$sql .= "INTO ".$this->protect_identifiers($table)." (" . PHP_EOL;

		// build fields
		$keys = array_keys($values);
		$keys = array_map(function($key){
			return sprintf('  "%s"', $key);
		},$keys);

		$sql .= implode(',' . PHP_EOL, $keys);

		$sql .= ") VALUES " .PHP_EOL;

		// handle multple
		if (is_array(next($values)))
		{
			foreach($values as $key => $val)
			{
				$sql .= '(';
				// TODO: format fields
				foreach($values as $key2 => $val2)
				{
					$temp[] = $this->escape($val2);
				}

				$sql .= implode(',' . PHP_EOL, $temp);
				$sql .= '), ' . PHP_EOL;
				unset($temp);
			}
		}
		else
		{
			$sql .= '('  . PHP_EOL;
			foreach($values as $key => $val)
			{	
				// format fields
				if( in_array($meta[$key]['type'], array('float')) )
				{
					if(!empty($val))
					{
						$temp[] = (float) $val;
					}
					else
					{
						$temp[] = 'NULL';
					}
					continue;
				}

				if( in_array($meta[$key]['type'], array('int', 'smallint')) )
				{
					$temp[] = (int) $val;
					continue;
				}

				$temp[] = $this->escape($val);
			}

			$sql .= implode(',' . PHP_EOL, $temp);

			$sql .= ')'  . PHP_EOL;
		}
		
		unset($temp);

		$sql .= ' ON CONFLICT ('. $primary_key .') '.PHP_EOL;
		$sql .= ' DO UPDATE SET '.PHP_EOL;

		foreach ($values as $key => $val) {

			if( in_array($meta[$key]['type'], array('float')) ){
				if(!empty($val))
				{
					$temp[] = sprintf('"%s" = %F', $key, $val);
				}
				else
				{
					$temp[] = sprintf('"%s" = %s', $key, 'NULL');
				}
				continue;
			}

			if( in_array($meta[$key]['type'], array('int', 'smallint')) )
			{
				$temp[] = sprintf('"%s" = %d', $key, $val);
				continue;
			}

			$temp[] = sprintf('"%s" = %s', $key, $this->escape($val));
		}

		$sql .= implode(','.PHP_EOL, $temp);

		// return ID
		$sql .= ' RETURNING '. $primary_key;

		#ddd($sql);
		

		$return = $this->query($sql);

		log_message('debug', $this->last_query());
		
		$this->_reset_write();

		$last_insert = $return->row_array();

		if (!empty($last_insert))
		{
			return $last_insert[$primary_key];
		}
		
		return $return;
	}

	// --------------------------------------------------------------------

	/**
	 * Allows you to get the compiled active record string without running the query
	 *
	 * @access	public
	 * @param	boolean	clear the active record
	 * @return	string
	 */
	public function get_query_string($clear = TRUE)
	{
		$sql = $this->_compile_select();
		if ($clear)
		{
			$this->clear_query_string();
		}
		return $sql;
	}


	// --------------------------------------------------------------------

	/**
	 * Clears the compiled query string
	 *
	 * @access	public
	 * @return	string
	 */
	public function clear_query_string()
	{
	   $this->_reset_select();
	}

	// --------------------------------------------------------------------

	/**
	 * Loads a SQL string and executes it... good for bigger data dumps
	 *
	 * @access	public
	 * @param	string	The path to a SQL file
	 * @param	boolean	If the contents being passed in parameter 1 is a path or a SQL string
	 * @return	void
	 */
	public function load_sql($sql_path, $is_path = TRUE)
	{
		$CI =& get_instance();
		// check first to see if it is a path to a file
		if (file_exists($sql_path) AND $is_path)
		{
			$sql = file_get_contents($sql_path);
		}

		// if not, assume it is a string
		else
		{
			$sql = $sql_path;
		}

		$sql = preg_replace('#^/\*(.+)\*/$#U', '', $sql);
		$sql = preg_replace('/^#(.+)$/U', '', $sql);

		// load database config
		if (file_exists(APPPATH.'config/'.ENVIRONMENT.'/database.php'))
		{
			include(APPPATH.'config/'.ENVIRONMENT.'/database.php');
		}
		else
		{
			include(APPPATH.'config/database.php');
		}

		$CI->load->database();

		// select the database
		$db = $db[$active_group]['database'];

		$use_sql = 'USE `'.$db.'`';

		$CI->db->query($use_sql);
		$sql_arr = explode(";\n", str_replace("\r\n", "\n", $sql));
		foreach($sql_arr as $s)
		{
			$s = trim($s);
			if (!empty($s))
			{
				$CI->db->query($s);
			}
		}
	}

	// --------------------------------------------------------------------

	/**
	 * Helps determine if there is currently a select specified for the active record
	 *
	 * @access	public
	 * @return	boolean
	 */
	public function has_select()
	{
		return !empty($this->qb_select);
	}

		/** http://stackoverflow.com/questions/6470267/grouping-where-clauses-in-codeigniter
	 * This function will allow you to do complex group where clauses in to c and (a AND b) or ( d and e)
	 * This function is needed as else the where clause will append an automatic AND in front of each where Thus if you wanted to do something
	 * like a AND ((b AND c) OR (d AND e)) you won't be able to as the where would insert it as a AND (AND (b...)) which is incorrect.
	 * Usage: start_group_where(key,value)->where(key,value)->close_group_where() or complex queries like
	 *        open_bracket()->start_group_where(key,value)->where(key,value)->close_group_where()
	 *        ->start_group_where(key,value,'','OR')->close_group_where()->close_bracket() would produce AND ((a AND b) OR (d))
	 * @param $key mixed the table columns prefix.columnname
	 * @param $value mixed the value of the key
	 * @param $escape string any escape as per CI
	 * @param $type the TYPE of query. By default it is set to 'AND'
	 * @return db object.
	 */
	public function start_group_where($key, $value = NULL, $escape = NULL, $type = "AND")
	{
		$this->open_bracket($type);
		return parent::_where($key, $value, '' ,$escape);
	}

	/**
	 * Strictly used to have a consistent close function as the start_group_where. This essentially callse the close_bracket() function.
	 */
	public function close_group_where()
	{
		return $this->close_bracket();
	}

	/**
	 * Allows to place a simple ( in a query and prepend it with the $type if needed.
	 * @param $type string add a ( to a query and prepend it with type. Default is $type.
	 * @param $return db object.
	 */
	public function open_bracket($type = "AND")
	{
		$this->ar_where[] = $type . " (";
		return $this;
	}

	/**
	 * Allows to place a simple ) to a query.
	 */
	public function close_bracket()
	{
		$this->ar_where[] = ")";
		return $this;
	}
}
/* End of file MY_DB_mysqli_driver.php */
/* Location: ./application/libraries/MY_DB_mysqli_driver.php */
