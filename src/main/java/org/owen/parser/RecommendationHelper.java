package org.owen.parser;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.owen.helper.DatabaseConnectionHelper;

public class RecommendationHelper {

	public List<String> getLocationFilter() {
		List<String> result = new ArrayList<>();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getLocationFilter()}"); ResultSet rs = cstmt.executeQuery()) {
			result.add("All");
			while (rs.next()) {
				result.add(rs.getString("location"));
			}
		} catch (Exception e) {
			Logger.getLogger(RecommendationHelper.class).error("Exception while fetching location filter", e);
		}
		return result;
	}

	public List<String> getExperienceFilter() {
		List<String> result = new ArrayList<>();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getExperienceFilter()}"); ResultSet rs = cstmt.executeQuery()) {
			result.add("All");
			while (rs.next()) {
				result.add(rs.getString("experience"));
			}
		} catch (Exception e) {
			Logger.getLogger(RecommendationHelper.class).error("Exception while fetching experience filter", e);
		}
		return result;
	}

	public List<String> getQualificationFilter() {
		List<String> result = new ArrayList<>();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getQualificationFilter()}");
				ResultSet rs = cstmt.executeQuery()) {
			result.add("All");
			while (rs.next()) {
				result.add(rs.getString("qualification"));
			}
		} catch (Exception e) {
			Logger.getLogger(RecommendationHelper.class).error("Exception while fetching qualification filter", e);
		}
		return result;
	}

	public List<String> getJobDescriptionFilter() {
		List<String> result = new ArrayList<>();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getJobDescriptionFilter()}");
				ResultSet rs = cstmt.executeQuery()) {
			while (rs.next()) {
				result.add(rs.getString("jd"));
			}
		} catch (Exception e) {
			Logger.getLogger(RecommendationHelper.class).error("Exception while fetching job description filter", e);
		}
		return result;
	}

	public String getRecommendations(String jobDescription, String location, String experience, String qualification) {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getRecommendations(?,?,?,?)}")) {
			cstmt.setString("in_jd", jobDescription);
			cstmt.setString("in_location", location);
			cstmt.setString("in_experience", experience);
			cstmt.setString("in_qualification", qualification);
			try (ResultSet rs = cstmt.executeQuery()) {
				while (rs.next()) {
					JSONObject jObj = new JSONObject();
					jObj.put("resultId", rs.getInt("result_id"));
					jObj.put("fileName", rs.getString("file_name"));
					jObj.put("filePath", rs.getString("file_path"));
					jObj.put("jd", rs.getString("jd"));
					jObj.put("status", rs.getString("status"));
					jObj.put("score", rs.getInt("score"));
					jObj.put("keywords", rs.getString("keywords"));
					jObj.put("experience", rs.getString("experience"));
					jObj.put("qualification", rs.getString("qualification"));
					result.put(jObj);
				}
			}
		} catch (Exception e) {
			Logger.getLogger(RecommendationHelper.class).error("Exception while fetching recommendations", e);
		}
		return result.toString();
	}

	public String getDashboardData() {
		JSONArray result = new JSONArray();
		DatabaseConnectionHelper dch = DatabaseConnectionHelper.getDBHelper();
		try (CallableStatement cstmt = dch.masterDS.getConnection().prepareCall("{call getRecommendations(?,?,?,?)}");
				ResultSet rs = cstmt.executeQuery()) {
			while (rs.next()) {
				JSONObject jObj = new JSONObject();
				jObj.put("resultId", rs.getInt("result_id"));
				jObj.put("fileName", rs.getString("file_name"));
				jObj.put("filePath", rs.getString("file_path"));
				jObj.put("jd", rs.getString("jd"));
				jObj.put("status", rs.getString("status"));
				jObj.put("score", rs.getInt("score"));
				jObj.put("keywords", rs.getString("keywords"));
				jObj.put("experience", rs.getString("experience"));
				jObj.put("qualification", rs.getString("qualification"));
				result.put(jObj);
			}
		} catch (Exception e) {
			Logger.getLogger(RecommendationHelper.class).error("Exception while fetching dashboard data", e);
		}
		return result.toString();
	}

}
