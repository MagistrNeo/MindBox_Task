import unittest
import math
from main_library import Triangle, Rectangle, is_right_angled_triangle, Circle, calculate_area

class TestGeometryLibrary(unittest.TestCase):
    
    def test_circle_area(self):
        """Тест вычисления площади круга"""
        circle = Circle(5)
        self.assertAlmostEqual(circle.area(), math.pi * 25)
        
        circle = Circle(1)
        self.assertAlmostEqual(circle.area(), math.pi)
    
    def test_circle_negative_radius(self):
        """Тест обработки отрицательного радиуса"""
        with self.assertRaises(ValueError):
            Circle(-1)
    
    def test_triangle_area(self):
        """Тест вычисления площади треугольника"""
        triangle = Triangle(3, 4, 5)
        self.assertAlmostEqual(triangle.area(), 6.0)
        
        triangle = Triangle(5, 5, 6)
        self.assertAlmostEqual(triangle.area(), 12.0)
    
    def test_triangle_validation(self):
        """Тест валидации сторон треугольника"""
        # Отрицательные стороны
        with self.assertRaises(ValueError):
            Triangle(-1, 2, 3)
        
        # Невозможный треугольник
        with self.assertRaises(ValueError):
            Triangle(1, 1, 3)
    
    def test_right_angled_triangle(self):
        """Тест проверки прямоугольного треугольника"""
        # Прямоугольный треугольник
        triangle = Triangle(3, 4, 5)
        self.assertTrue(triangle.is_right_angled())
        
        # Не прямоугольный треугольник
        triangle = Triangle(3, 4, 6)
        self.assertFalse(triangle.is_right_angled())
    
    def test_is_right_angled_triangle_function(self):
        """Тест утилитарной функции проверки прямоугольного треугольника"""
        self.assertTrue(is_right_angled_triangle(3, 4, 5))
        self.assertTrue(is_right_angled_triangle(5, 12, 13))
        self.assertFalse(is_right_angled_triangle(3, 4, 6))
    
    def test_calculate_area_polymorphism(self):
        """Тест полиморфного вычисления площади"""
        circle = Circle(2)
        triangle = Triangle(3, 4, 5)
        rectangle = Rectangle(4, 5)
        
        # Один интерфейс для разных фигур
        self.assertAlmostEqual(calculate_area(circle), math.pi * 4)
        self.assertAlmostEqual(calculate_area(triangle), 6.0)
        self.assertAlmostEqual(calculate_area(rectangle), 20.0)
    
    def test_rectangle_area(self):
        """Тест вычисления площади прямоугольника"""
        rectangle = Rectangle(4, 5)
        self.assertEqual(rectangle.area(), 20.0)
        self.assertTrue(rectangle.is_right_angled())
    
    def test_circle_not_right_angled(self):
        """Тест что круг не прямоугольный"""
        circle = Circle(5)
        self.assertFalse(circle.is_right_angled())

if __name__ == '__main__':
    unittest.main()