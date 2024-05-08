package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Posgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }


    @Override
    public List<String> getAllDistinctCategoryNames() {
        //Create a string with the sql statement
        String sql = "Select distinct name from category";
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of categories
            List<String> result = new ArrayList<String>();
            while (results.next()) {
                String test = results.getString("name");
                result.add(test);
            }
            //Return all the categories.
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX(int length) {
        String sql = "Select * from film where length > ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1,length);
            ResultSet results = statement.executeQuery();
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter) {
        //Create a string with the sql statement
        String sql = "Select * from actor where substring(first_name, 1, 1) = ?";
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            statement.setString(1, String.valueOf(firstLetter));
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of actors.
            List<Actor> actors = new ArrayList<>();
            //Loop through all the results and create a new Actor object to hold all its information
            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("ACTOR_ID"));
                actor.setFirstName(results.getString("FIRST_NAME"));
                actor.setLastName(results.getString("LAST_NAME"));
                actor.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add actor to the array
                actors.add(actor);
            }
            //Return all the actors.
            return actors;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Film> getFilmsInCategory(Category category) {
        String sql = "Select *\n" +
                "from film\n" +
                "join film_category\n" +
                "on (film.film_id = film_category.film_id)\n" +
                "join category\n" +
                "on (category.category_id = film_category.category_id)\n" +
                "where category.name = ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, String.valueOf(category));
            ResultSet results = statement.executeQuery();
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Actor> insertAllActorsWithAnOddNumberLastName(List<Actor> actors) {
        List<Actor> updatedActors = new ArrayList<>();

        String sql = "insert into actor (first_name, last_name) values (?, ?) returning actor_id, last_update;";
        for (Actor actor : actors) {
            if (actor.getLastName().length() % 2 != 0) {
                try (PreparedStatement statement = conn.prepareStatement(sql)) {
                    statement.setString(1, actor.getFirstName());
                    statement.setString(2, actor.getLastName());
                    ResultSet results = statement.executeQuery();
                    if (results.next()) {
                        actor.setActorId(results.getInt("ACTOR_ID"));
                        actor.setLastUpdate(results.getTimestamp("LAST_UPDATE"));
                        updatedActors.add(actor);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return updatedActors;
    }
}
