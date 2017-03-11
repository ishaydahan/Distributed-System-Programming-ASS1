import java.util.ArrayList;
import java.util.List;

public class job {

	public String local;
	public int n;
	public int d;
	public String start_date;
	public String end_date;
	public double speed;
	public double diameter;
	public double miss;	

	public int tasks;
	public int computers;
	public List<String> answers = new ArrayList<String>();

	public job(String local, int n, int d, String start_date, String end_date, double speed, double diameter, double miss){
		this.n=n;
		this.d=d;
		this.local=local;
		this.start_date=start_date;
		this.end_date=end_date;
		this.speed=speed;
		this.diameter=diameter;
		this.miss=miss;

	}

}
